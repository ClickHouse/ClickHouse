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

use arrow_array::cast::AsArray;
use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, ArrayRef as ArrowArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use futures::future::BoxFuture;
use futures::FutureExt;
use vortex::array::arrays::dict::DictArraySlotsExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::arrays::{Constant, Dict, Struct};
use vortex::array::buffer::BufferHandle;
use vortex::array::{ArrayRef, ExecutionCtx, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBufferMut};
use vortex::dtype::{DType, FieldName, Nullability};
use vortex::error::{vortex_err, VortexResult};
use vortex::expr::{get_item, is_null, lit, not, root, select, Expression};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::io::{IoBuf, VortexReadAt, VortexWrite};
use vortex::scalar::Scalar;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::operators::Operator;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Reads `length` bytes at `offset` into `out`. Returns 0 on success, non-zero on failure.
pub type VortexFFIReadCallback =
    unsafe extern "C" fn(context: *mut c_void, offset: u64, length: u64, out: *mut u8) -> i32;

/// Consumes `length` bytes from `data`. Returns 0 on success, non-zero on failure.
pub type VortexFFIWriteCallback =
    unsafe extern "C" fn(context: *mut c_void, data: *const u8, length: u64) -> i32;

pub struct VortexFFIReader {
    session: VortexSession,
    runtime: CurrentThreadRuntime,
    file: VortexFile,
    schema: SchemaRef,
}

pub struct VortexFFIScanner {
    session: VortexSession,
    /// The canonical Arrow schema of the projected columns; per-batch schemas may differ from it
    /// when an encoding is preserved (e.g. a dictionary-encoded column).
    schema: SchemaRef,
    chunks: Box<dyn Iterator<Item = VortexResult<ArrayRef>>>,
}

/// A node of a filter expression built through `vortex_ffi_expr_*`.
pub struct VortexFFIExpression(Expression);

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
            // The callback either fills all `length` bytes or fails, so the buffer does not need
            // to be zero-initialized.
            let mut buffer = ByteBufferMut::with_capacity_aligned(length, alignment);
            let result = unsafe {
                (this.read)(
                    this.context as *mut c_void,
                    offset,
                    length as u64,
                    buffer.spare_capacity_mut().as_mut_ptr().cast(),
                )
            };
            if result != 0 {
                return Err(vortex_err!("ClickHouse read callback failed"));
            }
            unsafe { buffer.set_len(length) };
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
            Ok(Box::into_raw(Box::new(VortexFFIReader { session, runtime, file, schema })))
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
/// with the given names are read, in the given order. If `filter` is not null, only the rows
/// matching the filter expression are returned; the file may use it to prune whole segments by
/// statistics and to decode only the matching rows. The reader must stay alive for the whole
/// lifetime of the scanner; `filter` is not consumed.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scanner_create(
    reader: *const VortexFFIReader,
    columns: *const *const c_char,
    num_columns: u64,
    filter: *const VortexFFIExpression,
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

            if !filter.is_null() {
                builder = builder.with_filter((*filter).0.clone());
            }

            let chunks = builder.into_iter(&reader.runtime).map_err(|e| e.to_string())?;
            Ok(Box::into_raw(Box::new(VortexFFIScanner {
                session: reader.session.clone(),
                schema,
                chunks: Box::new(chunks),
            })))
        })
    }
}

/// Returns the Arrow target field for exporting `array` while keeping its dictionary structure,
/// or `None` when the array should be exported canonically. Only arrays whose values are
/// scalar-like (numbers, booleans, strings, binaries) are exported as Arrow dictionaries,
/// because those are the types ClickHouse can read into a `LowCardinality` column.
fn dictionary_export_field(
    array: &ArrayRef,
    session: &VortexSession,
    name: &str,
) -> VortexResult<Option<Field>> {
    fn is_dictionary_value_dtype(dtype: &DType) -> bool {
        matches!(dtype, DType::Bool(_) | DType::Primitive(..) | DType::Utf8(_) | DType::Binary(_))
    }

    let make_field = |codes: &DType, values: &DType| -> VortexResult<Field> {
        let codes_type = session.arrow().to_arrow_field("", codes)?.data_type().clone();
        let values_type = session.arrow().to_arrow_field("", values)?.data_type().clone();
        Ok(Field::new(
            name,
            DataType::Dictionary(Box::new(codes_type), Box::new(values_type)),
            true,
        ))
    };

    if let Ok(dict) = array.clone().try_downcast::<Dict>() {
        if is_dictionary_value_dtype(dict.values().dtype()) {
            return Ok(Some(make_field(dict.codes().dtype(), dict.values().dtype())?));
        }
        return Ok(None);
    }

    if let Ok(constant) = array.clone().try_downcast::<Constant>() {
        let dtype = constant.scalar().dtype().clone();
        if !constant.scalar().is_null() && is_dictionary_value_dtype(&dtype) {
            let codes = DType::Primitive(vortex::dtype::PType::U8, Nullability::NonNullable);
            return Ok(Some(make_field(&codes, &dtype)?));
        }
        return Ok(None);
    }

    Ok(None)
}

/// Exports one scanned chunk as an Arrow struct array. Fields that are dictionary-encoded (or
/// constant) are exported as Arrow dictionary arrays instead of being fully decoded, so the
/// dictionary structure survives the format boundary; everything else is exported canonically.
fn export_chunk(
    chunk: ArrayRef,
    schema: &SchemaRef,
    session: &VortexSession,
    ctx: &mut ExecutionCtx,
) -> VortexResult<StructArray> {
    let chunk = match chunk.try_downcast::<Struct>() {
        Ok(struct_array) => {
            let mut fields = Vec::with_capacity(schema.fields().len());
            let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(schema.fields().len());
            for (i, field_array) in struct_array.iter_unmasked_fields().enumerate() {
                let schema_field = schema.field(i);
                let target = dictionary_export_field(field_array, session, schema_field.name())?;
                let arrow_array = session.arrow().execute_arrow(
                    field_array.clone(),
                    Some(target.as_ref().unwrap_or(schema_field)),
                    ctx,
                )?;
                fields.push(Arc::new(Field::new(
                    schema_field.name(),
                    arrow_array.data_type().clone(),
                    true,
                )));
                arrays.push(arrow_array);
            }
            return Ok(StructArray::new(Fields::from(fields), arrays, None));
        }
        Err(chunk) => chunk,
    };

    // The scanned chunk is not a plain struct array (e.g. it is chunked): export it canonically
    // as a whole.
    let struct_field = Field::new_struct("", schema.fields().clone(), false);
    let arrow = session.arrow().execute_arrow(chunk, Some(&struct_field), ctx)?;
    Ok(arrow.as_struct().clone())
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
            match scanner.chunks.next() {
                None => Ok(0),
                Some(chunk) => {
                    let chunk = chunk.map_err(|e| e.to_string())?;
                    let mut ctx = scanner.session.create_execution_ctx();
                    let struct_array =
                        export_chunk(chunk, &scanner.schema, &scanner.session, &mut ctx)
                            .map_err(|e| e.to_string())?;
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

/// The primitive type of a literal built through `vortex_ffi_expr_literal_*`. Must match the
/// exact type of the file column it is compared with: Vortex comparisons require both sides to
/// have the same type.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum VortexFFIPType {
    I8 = 0,
    I16 = 1,
    I32 = 2,
    I64 = 3,
    U8 = 4,
    U16 = 5,
    U32 = 6,
    U64 = 7,
    F32 = 8,
    F64 = 9,
}

/// A comparison operator for `vortex_ffi_expr_compare`.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum VortexFFIComparison {
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
}

/// The expression builders below return null on invalid input. They do not consume their
/// arguments: every returned handle must be freed with `vortex_ffi_expr_free`.

/// Creates an expression referencing the top-level column `name`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_column(name: *const c_char) -> *mut VortexFFIExpression {
    let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return std::ptr::null_mut();
    };
    let expr = get_item(FieldName::from(name), root());
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a signed integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_int(
    ptype: VortexFFIPType,
    value: i64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::I8 => match i8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I16 => match i16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I32 => match i32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates an unsigned integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_uint(
    ptype: VortexFFIPType,
    value: u64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::U8 => match u8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U16 => match u16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U32 => match u32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a floating-point literal of the given type. For `F32` the value must be exactly
/// representable as `f32`, otherwise null is returned (a rounded bound would change the
/// comparison result).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_float(
    ptype: VortexFFIPType,
    value: f64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::F32 => {
            let narrowed = value as f32;
            if f64::from(narrowed) != value {
                return std::ptr::null_mut();
            }
            Scalar::primitive(narrowed, nullability)
        }
        VortexFFIPType::F64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a boolean literal.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_bool(value: bool) -> *mut VortexFFIExpression {
    let scalar = Scalar::bool(value, Nullability::NonNullable);
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a string literal: `Utf8` if `is_utf8` is true (the bytes must be valid UTF-8,
/// otherwise null is returned), `Binary` otherwise.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_string(
    data: *const u8,
    length: u64,
    is_utf8: bool,
) -> *mut VortexFFIExpression {
    let bytes = unsafe { std::slice::from_raw_parts(data, length as usize) };
    let scalar = if is_utf8 {
        let Ok(string) = std::str::from_utf8(bytes) else {
            return std::ptr::null_mut();
        };
        Scalar::utf8(string.to_string(), Nullability::NonNullable)
    } else {
        Scalar::binary(bytes.to_vec(), Nullability::NonNullable)
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a comparison `lhs op rhs`. A comparison with a null value yields null, which the
/// scan treats as "row does not match".
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_compare(
    comparison: VortexFFIComparison,
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let operator = match comparison {
        VortexFFIComparison::Eq => Operator::Eq,
        VortexFFIComparison::NotEq => Operator::NotEq,
        VortexFFIComparison::Lt => Operator::Lt,
        VortexFFIComparison::Lte => Operator::Lte,
        VortexFFIComparison::Gt => Operator::Gt,
        VortexFFIComparison::Gte => Operator::Gte,
    };
    let expr =
        unsafe { Binary.new_expr(operator, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a Kleene (three-valued) AND of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_and(
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr =
        unsafe { Binary.new_expr(Operator::And, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a Kleene (three-valued) OR of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_or(
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr =
        unsafe { Binary.new_expr(Operator::Or, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a logical NOT of a boolean expression (NOT of null is null).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_not(
    child: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { not((*child).0.clone()) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates an expression that is true where the child expression is null.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_is_null(
    child: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { is_null((*child).0.clone()) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_free(expr: *mut VortexFFIExpression) {
    if !expr.is_null() {
        unsafe { drop(Box::from_raw(expr)) };
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

            let scanner =
                vortex_ffi_scanner_create(reader, std::ptr::null(), 0, std::ptr::null(), &mut error);
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
            let scanner =
                vortex_ffi_scanner_create(reader, columns.as_ptr(), 1, std::ptr::null(), &mut error);
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

            // Read with a pushed-down filter: id > 2 matches rows 3, 4 and 5.
            let id = CString::new("id").expect("valid name");
            let id_column = vortex_ffi_expr_column(id.as_ptr());
            let threshold = vortex_ffi_expr_literal_int(VortexFFIPType::I64, 2);
            let filter = vortex_ffi_expr_compare(VortexFFIComparison::Gt, id_column, threshold);
            assert!(!filter.is_null());
            let scanner =
                vortex_ffi_scanner_create(reader, std::ptr::null(), 0, filter, &mut error);
            assert!(!scanner.is_null(), "{:?}", CStr::from_ptr(error));
            vortex_ffi_expr_free(filter);
            vortex_ffi_expr_free(threshold);
            vortex_ffi_expr_free(id_column);

            let mut filtered_rows = 0;
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
                filtered_rows += batch.num_rows();
            }
            assert_eq!(filtered_rows, 3);
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
