use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::Stream;
use futures::StreamExt;
use lance::Dataset;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use tokio::runtime::Runtime;

#[repr(C)]
pub struct ch_lance_error {
    message: *mut c_char,
}

#[repr(C)]
pub struct ch_lance_dataset_options {
    uri: *const c_char,
}

#[repr(C)]
pub struct ch_lance_snapshot_info {
    snapshot_id: u64,
    schema_id: u64,
}

#[repr(C)]
pub struct ch_lance_string_list {
    values: *const *const c_char,
    size: usize,
}

#[repr(C)]
pub struct ch_lance_scan_options {
    snapshot_id: u64,
    projection: ch_lance_string_list,
    predicate: *const c_char,
    need_only_count: bool,
    max_block_size: u64,
}

#[repr(C)]
pub struct ch_lance_dataset {
    runtime: Runtime,
    dataset: Dataset,
}

#[repr(C)]
pub struct ch_lance_scan {
    runtime: Runtime,
    stream: Pin<Box<dyn Stream<Item = lance::Result<arrow_array::RecordBatch>> + Send>>,
}

fn set_error(error: *mut ch_lance_error, message: &str) {
    if error.is_null() {
        return;
    }

    let message = CString::new(message)
        .unwrap_or_else(|_| CString::new("Lance error contains an interior null byte").unwrap());
    unsafe {
        (*error).message = message.into_raw();
    }
}

fn clear_error(error: *mut ch_lance_error) {
    if !error.is_null() {
        unsafe {
            (*error).message = std::ptr::null_mut();
        }
    }
}

fn cstr_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Ok(String::new());
    }

    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|err| err.to_string())
}

fn required_cstr_to_string(ptr: *const c_char, name: &str) -> Result<String, String> {
    let value = cstr_to_string(ptr)?;
    if value.is_empty() {
        Err(format!("Lance dataset option `{}` must not be empty", name))
    } else {
        Ok(value)
    }
}

unsafe fn apply_dataset_options(options: &ch_lance_dataset_options) -> Result<String, String> {
    required_cstr_to_string(options.uri, "uri")
}

fn projection_from_ffi(list: &ch_lance_string_list) -> Result<Vec<String>, String> {
    if list.size == 0 {
        return Ok(Vec::new());
    }
    if list.values.is_null() {
        return Err("Lance projection list is null but size is non-zero".to_string());
    }

    let mut result = Vec::with_capacity(list.size);
    for index in 0..list.size {
        let value_ptr = unsafe { *list.values.add(index) };
        result.push(required_cstr_to_string(value_ptr, "projection")?);
    }
    Ok(result)
}

fn write_schema(schema: arrow_schema::Schema, out: *mut FFI_ArrowSchema) -> Result<(), String> {
    if out.is_null() {
        return Err("ArrowSchema output pointer is null".to_string());
    }

    let ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|err| err.to_string())?;
    unsafe {
        std::ptr::write_unaligned(out, ffi_schema);
    }
    Ok(())
}

fn validate_schema(schema: &Schema) -> Result<(), String> {
    for field in schema.fields() {
        validate_field(field)?;
    }
    Ok(())
}

fn validate_field(field: &Field) -> Result<(), String> {
    if field.metadata().contains_key("ARROW:extension:name") {
        return Err(format!(
            "Unsupported Lance column `{}`: Arrow extension types are not supported by the read-only MVP",
            field.name()
        ));
    }
    validate_data_type(field.name(), field.data_type())
}

fn validate_data_type(column_name: &str, data_type: &DataType) -> Result<(), String> {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(()),
        DataType::List(child) | DataType::LargeList(child) => match child.data_type() {
            DataType::Float32 => Ok(()),
            other => Err(format!(
                "Unsupported Lance column `{}`: only Array(Float32) is supported for list columns, got {}",
                column_name, other
            )),
        },
        DataType::FixedSizeList(child, _) => match child.data_type() {
            DataType::Float32 => Ok(()),
            other => Err(format!(
                "Unsupported Lance column `{}`: only Array(Float32) is supported for fixed-size list columns, got {}",
                column_name, other
            )),
        },
        other => Err(format!(
            "Unsupported Lance column `{}`: Arrow type {} is not supported by the read-only MVP",
            column_name, other
        )),
    }
}

fn write_record_batch(
    batch: arrow_array::RecordBatch,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<(), String> {
    if array.is_null() {
        return Err("ArrowArray output pointer is null".to_string());
    }
    if schema.is_null() {
        return Err("ArrowSchema output pointer is null".to_string());
    }

    validate_schema(batch.schema().as_ref())?;
    let ffi_schema =
        FFI_ArrowSchema::try_from(batch.schema().as_ref()).map_err(|err| err.to_string())?;
    let struct_array = StructArray::from(batch);
    let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());

    unsafe {
        std::ptr::write_unaligned(array, ffi_array);
        std::ptr::write_unaligned(schema, ffi_schema);
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_open_dataset(
    options: *const ch_lance_dataset_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_dataset {
    clear_error(error);
    if options.is_null() {
        set_error(error, "Lance dataset options pointer is null");
        return std::ptr::null_mut();
    }

    let uri = match apply_dataset_options(&*options) {
        Ok(uri) => uri,
        Err(message) => {
            set_error(error, &message);
            return std::ptr::null_mut();
        }
    };

    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            set_error(error, &format!("Cannot create Lance runtime: {}", err));
            return std::ptr::null_mut();
        }
    };

    match runtime.block_on(Dataset::open(&uri)) {
        Ok(dataset) => Box::into_raw(Box::new(ch_lance_dataset { runtime, dataset })),
        Err(err) => {
            set_error(error, &err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_dataset(dataset: *mut ch_lance_dataset) {
    if !dataset.is_null() {
        drop(Box::from_raw(dataset));
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_current_snapshot(
    dataset: *mut ch_lance_dataset,
    snapshot: *mut ch_lance_snapshot_info,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || snapshot.is_null() {
        set_error(error, "Lance dataset or snapshot pointer is null");
        return false;
    }

    let dataset = &*dataset;
    let version = dataset.dataset.version().version;
    (*snapshot).snapshot_id = version;
    (*snapshot).schema_id = 0;
    true
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_export_schema(
    dataset: *mut ch_lance_dataset,
    snapshot_id: u64,
    schema: *mut FFI_ArrowSchema,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() {
        set_error(error, "Lance dataset pointer is null");
        return false;
    }

    let dataset = &mut *dataset;
    let dataset = if snapshot_id == 0 || snapshot_id == dataset.dataset.version().version {
        dataset.dataset.clone()
    } else {
        match dataset
            .runtime
            .block_on(dataset.dataset.checkout_version(snapshot_id))
        {
            Ok(dataset) => dataset,
            Err(err) => {
                set_error(error, &err.to_string());
                return false;
            }
        }
    };
    let arrow_schema = dataset.schema().into();
    match validate_schema(&arrow_schema).and_then(|()| write_schema(arrow_schema, schema)) {
        Ok(()) => true,
        Err(message) => {
            set_error(error, &message);
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_total_rows(
    dataset: *mut ch_lance_dataset,
    snapshot_id: u64,
    rows: *mut u64,
    has_value: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || rows.is_null() || has_value.is_null() {
        set_error(error, "Lance dataset, rows, or has_value pointer is null");
        return false;
    }

    let dataset = &mut *dataset;
    let count_result = dataset.runtime.block_on(async {
        let dataset = if snapshot_id == 0 || snapshot_id == dataset.dataset.version().version {
            dataset.dataset.clone()
        } else {
            dataset.dataset.checkout_version(snapshot_id).await?
        };
        dataset.count_rows(None).await
    });

    match count_result {
        Ok(count) => {
            *rows = count as u64;
            *has_value = true;
            true
        }
        Err(err) => {
            set_error(error, &err.to_string());
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_total_bytes(
    _dataset: *mut ch_lance_dataset,
    _bytes: *mut u64,
    has_value: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if !has_value.is_null() {
        *has_value = false;
    }
    true
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_plan_scan(
    dataset: *mut ch_lance_dataset,
    options: *const ch_lance_scan_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_scan {
    clear_error(error);
    if dataset.is_null() || options.is_null() {
        set_error(error, "Lance dataset or scan options pointer is null");
        return std::ptr::null_mut();
    }

    let projection = match projection_from_ffi(&(*options).projection) {
        Ok(projection) => projection,
        Err(message) => {
            set_error(error, &message);
            return std::ptr::null_mut();
        }
    };
    let predicate = match cstr_to_string((*options).predicate) {
        Ok(predicate) => predicate,
        Err(message) => {
            set_error(error, &message);
            return std::ptr::null_mut();
        }
    };
    let snapshot_id = (*options).snapshot_id;
    let max_block_size = (*options).max_block_size as usize;
    let source_dataset = (*dataset).dataset.clone();

    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            set_error(error, &format!("Cannot create Lance scan runtime: {}", err));
            return std::ptr::null_mut();
        }
    };

    let stream_result = runtime.block_on(async move {
        let dataset = if snapshot_id == 0 || snapshot_id == source_dataset.version().version {
            source_dataset
        } else {
            source_dataset.checkout_version(snapshot_id).await?
        };

        let mut scanner = dataset.scan();
        if !projection.is_empty() {
            scanner.project(&projection)?;
        }
        if !predicate.is_empty() {
            scanner.filter(&predicate)?;
        }
        if max_block_size != 0 {
            scanner.batch_size(max_block_size);
        }
        scanner.try_into_stream().await
    });

    match stream_result {
        Ok(stream) => Box::into_raw(Box::new(ch_lance_scan {
            runtime,
            stream: Box::pin(stream),
        })),
        Err(err) => {
            set_error(error, &err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_next_batch(
    scan: *mut ch_lance_scan,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    has_batch: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if scan.is_null() || has_batch.is_null() {
        set_error(error, "Lance scan or has_batch pointer is null");
        return false;
    }

    let scan = &mut *scan;
    match scan.runtime.block_on(scan.stream.as_mut().next()) {
        None => {
            *has_batch = false;
            true
        }
        Some(Ok(batch)) => {
            *has_batch = true;
            match write_record_batch(batch, array, schema) {
                Ok(()) => true,
                Err(message) => {
                    set_error(error, &message);
                    false
                }
            }
        }
        Some(Err(err)) => {
            set_error(error, &err.to_string());
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_scan(scan: *mut ch_lance_scan) {
    if !scan.is_null() {
        drop(Box::from_raw(scan));
    }
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_error(error: *mut ch_lance_error) {
    if error.is_null() {
        return;
    }

    let message = (*error).message;
    if !message.is_null() {
        drop(CString::from_raw(message));
        (*error).message = ptr::null_mut();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ffi::from_ffi;
    use arrow_array::ffi::FFI_ArrowArray;
    use arrow_array::{make_array, Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use std::ptr::addr_of_mut;
    use std::sync::Arc;

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef,
        )])
        .unwrap();
        batch
    }

    fn write_test_dataset() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(vec![1, 2, 3]);
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(reader, dir.path().to_str().unwrap(), None))
            .unwrap();
        dir
    }

    #[test]
    fn ffi_open_schema_and_scan_local_dataset() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = ch_lance_dataset_options { uri: uri.as_ptr() };
        let mut error = ch_lance_error {
            message: ptr::null_mut(),
        };

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info {
            snapshot_id: 0,
            schema_id: 0,
        };
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });
        assert!(snapshot.snapshot_id > 0);

        let mut schema = FFI_ArrowSchema::empty();
        assert!(unsafe {
            ch_lance_export_schema(
                dataset,
                snapshot.snapshot_id,
                addr_of_mut!(schema),
                addr_of_mut!(error),
            )
        });

        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let predicate = CString::new("id = 2").unwrap();
        let scan_options = ch_lance_scan_options {
            snapshot_id: snapshot.snapshot_id,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: predicate.as_ptr(),
            need_only_count: false,
            max_block_size: 1024,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(!scan.is_null());

        let mut array = FFI_ArrowArray::empty();
        let mut batch_schema = FFI_ArrowSchema::empty();
        let mut has_batch = false;
        assert!(unsafe {
            ch_lance_next_batch(
                scan,
                addr_of_mut!(array),
                addr_of_mut!(batch_schema),
                addr_of_mut!(has_batch),
                addr_of_mut!(error),
            )
        });
        assert!(has_batch);

        let struct_data = unsafe { from_ffi(array, &batch_schema) }.unwrap();
        let struct_array = StructArray::from(make_array(struct_data).to_data());
        assert_eq!(struct_array.len(), 1);

        unsafe {
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_total_rows_uses_requested_snapshot() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = ch_lance_dataset_options { uri: uri.as_ptr() };
        let mut error = ch_lance_error {
            message: ptr::null_mut(),
        };

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info {
            snapshot_id: 0,
            schema_id: 0,
        };
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let mut rows = 0;
        let mut has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                dataset,
                snapshot.snapshot_id,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 3);

        let append_batch = make_batch(vec![4]);
        let schema = append_batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(append_batch)].into_iter(), schema);
        Runtime::new().unwrap().block_on(async {
            let mut dataset = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
            dataset.append(reader, None).await.unwrap();
        });

        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                dataset,
                snapshot.snapshot_id,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 3);

        let latest_dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!latest_dataset.is_null());
        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                latest_dataset,
                0,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 4);

        unsafe {
            ch_lance_free_dataset(latest_dataset);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn unsupported_map_type_reports_clear_error() {
        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]);

        let error = validate_schema(&schema).unwrap_err();
        assert!(error.contains("Unsupported Lance column `m`"));
        assert!(error.contains("not supported by the read-only MVP"));
    }
}
