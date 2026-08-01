//! C FFI bindings for the Vortex columnar file format (https://github.com/vortex-data/vortex),
//! used by ClickHouse's `Vortex` input/output formats.
//!
//! Data crosses the FFI boundary through the Arrow C Data Interface: the reader exports each
//! scanned chunk as a `struct ArrowArray` + `struct ArrowSchema` pair, and the writer accepts
//! record batches in the same representation. IO is delegated back to ClickHouse through
//! callbacks, so all reads and writes go through ClickHouse's own buffers (local files, S3,
//! HTTP, throttling, and so on).
//!
//! Threading model: all work is driven by a `CurrentThreadRuntime`, which makes progress only
//! inside the FFI calls, on the calling thread. No background threads are spawned, and the IO
//! callbacks are only ever invoked from within an FFI call. Handles must not be used from
//! multiple threads concurrently, but may be moved between threads.
//!
//! Error convention: fallible functions take a `char ** error` out-parameter. On failure it is
//! set to a heap-allocated message that must be freed with `vortex_ffi_free_string`.

use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::{Schema, SchemaRef};
use futures::future::BoxFuture;
use futures::FutureExt;
use vortex::array::buffer::BufferHandle;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBufferMut};
use vortex::dtype::FieldName;
use vortex::error::{vortex_err, VortexResult};
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::io::{IoBuf, VortexReadAt, VortexWrite};
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Reads `length` bytes at `offset` into `out`. Returns 0 on success, non-zero on failure.
pub type VortexFFIReadCallback =
    unsafe extern "C" fn(context: *mut c_void, offset: u64, length: u64, out: *mut u8) -> i32;

/// Consumes `length` bytes from `data`. Returns 0 on success, non-zero on failure.
pub type VortexFFIWriteCallback =
    unsafe extern "C" fn(context: *mut c_void, data: *const u8, length: u64) -> i32;

pub struct VortexFFIReader {
    runtime: CurrentThreadRuntime,
    file: VortexFile,
    schema: SchemaRef,
}

pub struct VortexFFIScanner {
    batches: Box<dyn RecordBatchReader>,
}

pub struct VortexFFIWriter {
    session: VortexSession,
    runtime: CurrentThreadRuntime,
    schema: SchemaRef,
    writer: Option<vortex::file::Writer<'static>>,
}

unsafe fn set_error(error: *mut *mut c_char, message: String)
{
    if error.is_null() {
        return;
    }
    let message = CString::new(message.replace('\0', " "))
        .unwrap_or_else(|_| CString::new("invalid error message").expect("valid literal"));
    unsafe {
        *error = message.into_raw();
    }
}

/// Runs `f`, catching both errors and panics. On failure stores the message into `error` and
/// returns `on_error`.
unsafe fn ffi_wrap<T, F>(error: *mut *mut c_char, on_error: T, f: F) -> T
where
    F: FnOnce() -> Result<T, String>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            unsafe { set_error(error, message) };
            on_error
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            unsafe { set_error(error, format!("panic: {message}")) };
            on_error
        }
    }
}

/// A `VortexReadAt` implementation on top of a ClickHouse read callback.
///
/// `concurrency` is 1 and the runtime is single-threaded, so the callback is never invoked
/// concurrently, and only from within an FFI call.
#[derive(Clone)]
struct CallbackReader {
    context: usize,
    read: VortexFFIReadCallback,
    size: u64,
}

impl VortexReadAt for CallbackReader {
    fn concurrency(&self) -> usize {
        1
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let size = self.size;
        async move { Ok(size) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let this = self.clone();
        async move {
            if offset.checked_add(length as u64).is_none_or(|end| end > this.size) {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
            }
            let mut buffer = ByteBufferMut::zeroed_aligned(length, alignment);
            let result = unsafe {
                (this.read)(
                    this.context as *mut c_void,
                    offset,
                    length as u64,
                    buffer.as_mut_slice().as_mut_ptr(),
                )
            };
            if result != 0 {
                return Err(vortex_err!("ClickHouse read callback failed"));
            }
            Ok(BufferHandle::new_host(buffer.freeze()))
        }
        .boxed()
    }
}

/// A `VortexWrite` implementation on top of a ClickHouse write callback.
struct CallbackWrite {
    context: usize,
    write: VortexFFIWriteCallback,
}

impl VortexWrite for CallbackWrite {
    fn write_all<B: IoBuf>(&mut self, buffer: B) -> impl std::future::Future<Output = std::io::Result<B>> {
        let slice = buffer.as_slice();
        let result = unsafe { (self.write)(self.context as *mut c_void, slice.as_ptr(), slice.len() as u64) };
        std::future::ready(if result == 0 {
            Ok(buffer)
        } else {
            Err(std::io::Error::other("ClickHouse write callback failed"))
        })
    }

    fn flush(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }
}

fn make_session(runtime: &CurrentThreadRuntime) -> VortexSession {
    VortexSession::default().with_handle(runtime.handle())
}

/// Opens a Vortex file for reading. The file is accessed through `read` with the given opaque
/// `context`; `file_size` must be the exact file size in bytes.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_open(
    context: *mut c_void,
    read: VortexFFIReadCallback,
    file_size: u64,
    error: *mut *mut c_char,
) -> *mut VortexFFIReader {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let runtime = CurrentThreadRuntime::new();
            let session = make_session(&runtime);
            let source = CallbackReader { context: context as usize, read, size: file_size };
            let file = runtime
                .block_on(session.open_options().with_file_size(file_size).open_read(source))
                .map_err(|e| e.to_string())?;
            let schema = Arc::new(
                session
                    .arrow()
                    .to_arrow_schema(file.dtype())
                    .map_err(|e| e.to_string())?,
            );
            Ok(Box::into_raw(Box::new(VortexFFIReader { runtime, file, schema })))
        })
    }
}

/// Returns the total number of rows in the file.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_row_count(reader: *const VortexFFIReader) -> u64 {
    unsafe { (*reader).file.row_count() }
}

/// Exports the file schema into `out_schema` (an Arrow C Data Interface `struct ArrowSchema`).
/// The caller takes ownership and must release it. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_schema(
    reader: *const VortexFFIReader,
    out_schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let reader = &*reader;
            let ffi_schema =
                FFI_ArrowSchema::try_from(reader.schema.as_ref()).map_err(|e| e.to_string())?;
            std::ptr::write(out_schema, ffi_schema);
            Ok(0)
        })
    }
}

/// Creates a scanner over the file. If `columns` is not null, only the `num_columns` columns
/// with the given names are read, in the given order. The reader must stay alive for the whole
/// lifetime of the scanner.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scanner_create(
    reader: *const VortexFFIReader,
    columns: *const *const c_char,
    num_columns: u64,
    error: *mut *mut c_char,
) -> *mut VortexFFIScanner {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let reader = &*reader;
            let mut builder = reader.file.scan().map_err(|e| e.to_string())?;

            let schema = if columns.is_null() {
                reader.schema.clone()
            } else {
                let mut names = Vec::with_capacity(num_columns as usize);
                for i in 0..num_columns {
                    let name = CStr::from_ptr(*columns.add(i as usize))
                        .to_str()
                        .map_err(|e| e.to_string())?;
                    names.push(name.to_string());
                }
                let mut fields = Vec::with_capacity(names.len());
                for name in &names {
                    fields.push(
                        reader.schema.field_with_name(name).map_err(|e| e.to_string())?.clone(),
                    );
                }
                let field_names: Vec<FieldName> =
                    names.iter().map(|name| FieldName::from(name.as_str())).collect();
                builder = builder.with_projection(select(field_names, root()));
                Arc::new(Schema::new(fields))
            };

            let batches = builder
                .into_record_batch_reader(schema, &reader.runtime)
                .map_err(|e| e.to_string())?;
            Ok(Box::into_raw(Box::new(VortexFFIScanner { batches: Box::new(batches) })))
        })
    }
}

/// Reads the next batch of rows into `out_array` + `out_schema` (Arrow C Data Interface). The
/// caller takes ownership of both and must release them. Returns 1 if a batch was produced,
/// 0 at the end of the file, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scanner_next(
    scanner: *mut VortexFFIScanner,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let scanner = &mut *scanner;
            match scanner.batches.next() {
                None => Ok(0),
                Some(batch) => {
                    let batch = batch.map_err(|e| e.to_string())?;
                    let struct_array = StructArray::from(batch);
                    let (ffi_array, ffi_schema) =
                        to_ffi(&struct_array.into_data()).map_err(|e| e.to_string())?;
                    std::ptr::write(out_array, ffi_array);
                    std::ptr::write(out_schema, ffi_schema);
                    Ok(1)
                }
            }
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scanner_free(scanner: *mut VortexFFIScanner) {
    if !scanner.is_null() {
        unsafe { drop(Box::from_raw(scanner)) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_free(reader: *mut VortexFFIReader) {
    if !reader.is_null() {
        unsafe { drop(Box::from_raw(reader)) };
    }
}

/// Creates a writer producing a Vortex file with the given schema (consumed). The bytes of the
/// file are sent to `write` with the given opaque `context`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_create(
    context: *mut c_void,
    write: VortexFFIWriteCallback,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> *mut VortexFFIWriter {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let ffi_schema = std::ptr::read(schema);
            let arrow_schema = Schema::try_from(&ffi_schema).map_err(|e| e.to_string())?;
            let runtime = CurrentThreadRuntime::new();
            let session = make_session(&runtime);
            let dtype =
                session.arrow().from_arrow_schema(&arrow_schema).map_err(|e| e.to_string())?;
            let sink = CallbackWrite { context: context as usize, write };
            let writer = session.write_options().writer(sink, dtype);
            Ok(Box::into_raw(Box::new(VortexFFIWriter {
                session,
                runtime,
                schema: Arc::new(arrow_schema),
                writer: Some(writer),
            })))
        })
    }
}

/// Appends one record batch (Arrow C Data Interface, consumed) to the file. The batch must have
/// the same schema the writer was created with. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_write(
    writer: *mut VortexFFIWriter,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let writer = &mut *writer;
            let ffi_array = std::ptr::read(array);
            let ffi_schema = std::ptr::read(schema);
            let data = from_ffi(ffi_array, &ffi_schema).map_err(|e| e.to_string())?;
            let batch = RecordBatch::from(StructArray::from(data));
            let chunk = writer
                .session
                .arrow()
                .from_arrow_record_batch(batch, &writer.schema)
                .map_err(|e| e.to_string())?;
            let vortex_writer = writer
                .writer
                .as_mut()
                .ok_or_else(|| "writer is already finished".to_string())?;
            writer
                .runtime
                .block_on(vortex_writer.push(chunk))
                .map_err(|e| e.to_string())?;
            Ok(0)
        })
    }
}

/// Flushes the remaining data and writes the file footer. Must be called exactly once before
/// `vortex_ffi_writer_free`. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_finish(
    writer: *mut VortexFFIWriter,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let writer = &mut *writer;
            let vortex_writer =
                writer.writer.take().ok_or_else(|| "writer is already finished".to_string())?;
            writer
                .runtime
                .block_on(vortex_writer.finish())
                .map_err(|e| e.to_string())?;
            Ok(0)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_free(writer: *mut VortexFFIWriter) {
    if !writer.is_null() {
        unsafe { drop(Box::from_raw(writer)) };
    }
}

/// Frees a string returned by this library (for example an error message).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_free_string(string: *mut c_char) {
    if !string.is_null() {
        unsafe { drop(CString::from_raw(string)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn write_to_vec(context: *mut c_void, data: *const u8, length: u64) -> i32 {
        let out = unsafe { &mut *(context as *mut Vec<u8>) };
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(data, length as usize) });
        0
    }

    unsafe extern "C" fn read_from_vec(
        context: *mut c_void,
        offset: u64,
        length: u64,
        out: *mut u8,
    ) -> i32 {
        let data = unsafe { &*(context as *const Vec<u8>) };
        let Some(end) = offset.checked_add(length) else { return 1 };
        if end > data.len() as u64 {
            return 1;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr().add(offset as usize),
                out,
                length as usize,
            )
        };
        0
    }

    fn test_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(names))],
        )
        .expect("valid batch")
    }

    #[test]
    fn ffi_roundtrip() {
        let mut file = Vec::<u8>::new();
        let mut error: *mut c_char = std::ptr::null_mut();

        // Write a file with two batches.
        unsafe {
            let batch = test_batch(vec![1, 2, 3], vec![Some("a"), None, Some("c")]);
            let mut ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).expect("schema");
            let writer = vortex_ffi_writer_create(
                &mut file as *mut Vec<u8> as *mut c_void,
                write_to_vec,
                &mut ffi_schema,
                &mut error,
            );
            std::mem::forget(ffi_schema);
            assert!(!writer.is_null(), "{:?}", CStr::from_ptr(error));

            for batch in [
                test_batch(vec![1, 2, 3], vec![Some("a"), None, Some("c")]),
                test_batch(vec![4, 5], vec![Some("d"), Some("e")]),
            ] {
                let (mut ffi_array, mut ffi_schema) =
                    to_ffi(&StructArray::from(batch).into_data()).expect("to_ffi");
                let result =
                    vortex_ffi_writer_write(writer, &mut ffi_array, &mut ffi_schema, &mut error);
                std::mem::forget(ffi_array);
                std::mem::forget(ffi_schema);
                assert_eq!(result, 0, "{:?}", CStr::from_ptr(error));
            }
            assert_eq!(vortex_ffi_writer_finish(writer, &mut error), 0);
            vortex_ffi_writer_free(writer);
        }

        assert_eq!(&file[0..4], b"VTXF");

        // Read the whole file back.
        unsafe {
            let reader = vortex_ffi_reader_open(
                &mut file as *mut Vec<u8> as *mut c_void,
                read_from_vec,
                file.len() as u64,
                &mut error,
            );
            assert!(!reader.is_null(), "{:?}", CStr::from_ptr(error));
            assert_eq!(vortex_ffi_reader_row_count(reader), 5);

            let mut ffi_schema = FFI_ArrowSchema::empty();
            assert_eq!(vortex_ffi_reader_schema(reader, &mut ffi_schema, &mut error), 0);
            let schema = Schema::try_from(&ffi_schema).expect("schema");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "name");

            let scanner = vortex_ffi_scanner_create(reader, std::ptr::null(), 0, &mut error);
            assert!(!scanner.is_null(), "{:?}", CStr::from_ptr(error));

            let mut total_rows = 0;
            loop {
                let mut out_array = FFI_ArrowArray::empty();
                let mut out_schema = FFI_ArrowSchema::empty();
                let result =
                    vortex_ffi_scanner_next(scanner, &mut out_array, &mut out_schema, &mut error);
                assert_ne!(result, -1, "{:?}", CStr::from_ptr(error));
                if result == 0 {
                    break;
                }
                let data = from_ffi(out_array, &out_schema).expect("from_ffi");
                let batch = RecordBatch::from(StructArray::from(data));
                total_rows += batch.num_rows();
            }
            assert_eq!(total_rows, 5);
            vortex_ffi_scanner_free(scanner);

            // Read a single projected column.
            let column = CString::new("name").expect("valid name");
            let columns = [column.as_ptr()];
            let scanner = vortex_ffi_scanner_create(reader, columns.as_ptr(), 1, &mut error);
            assert!(!scanner.is_null(), "{:?}", CStr::from_ptr(error));
            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            let result =
                vortex_ffi_scanner_next(scanner, &mut out_array, &mut out_schema, &mut error);
            assert_eq!(result, 1, "{:?}", CStr::from_ptr(error));
            let data = from_ffi(out_array, &out_schema).expect("from_ffi");
            let batch = RecordBatch::from(StructArray::from(data));
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "name");
            vortex_ffi_scanner_free(scanner);

            vortex_ffi_reader_free(reader);
        }

        // A truncated file must produce an error, not a panic or a crash.
        unsafe {
            let mut truncated = file[0..file.len() / 2].to_vec();
            let reader = vortex_ffi_reader_open(
                &mut truncated as *mut Vec<u8> as *mut c_void,
                read_from_vec,
                truncated.len() as u64,
                &mut error,
            );
            assert!(reader.is_null());
            assert!(!error.is_null());
            vortex_ffi_free_string(error);
        }
    }
}
