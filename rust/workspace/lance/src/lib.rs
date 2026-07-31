use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use lance::Dataset;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::pin::Pin;
use std::ptr;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[repr(C)]
pub struct ch_lance_error {
    message: *mut c_char,
}

#[repr(C)]
pub struct ch_lance_dataset_options {
    uri: *const c_char,
    use_s3: bool,
    s3_region: *const c_char,
    s3_endpoint: *const c_char,
    s3_access_key_id: *const c_char,
    s3_secret_access_key: *const c_char,
    s3_session_token: *const c_char,
    s3_role_arn: *const c_char,
    s3_role_session_name: *const c_char,
    s3_use_environment_credentials: bool,
    s3_no_sign_request: bool,
    s3_allow_http: bool,
    s3_virtual_hosted_style_request: bool,
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
    _s3_cache: Option<TempDir>,
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

#[derive(Clone)]
struct DatasetOpenOptions {
    uri: String,
    storage_options: Option<HashMap<String, String>>,
}

struct OpenedDataset {
    dataset: Dataset,
    s3_cache: Option<TempDir>,
}

unsafe fn apply_dataset_options(
    options: &ch_lance_dataset_options,
) -> Result<DatasetOpenOptions, String> {
    let uri = required_cstr_to_string(options.uri, "uri")?;
    let storage_options = if options.use_s3 {
        let mut values = HashMap::new();

        let fields = [
            ("aws_region", options.s3_region),
            ("aws_endpoint", options.s3_endpoint),
            ("aws_access_key_id", options.s3_access_key_id),
            ("aws_secret_access_key", options.s3_secret_access_key),
            ("aws_session_token", options.s3_session_token),
            ("aws_role_arn", options.s3_role_arn),
            ("aws_role_session_name", options.s3_role_session_name),
        ];
        for (name, value_ptr) in fields {
            let value = cstr_to_string(value_ptr)?;
            if !value.is_empty() {
                values.insert(name.to_string(), value);
            }
        }
        if options.s3_no_sign_request {
            values.insert("aws_skip_signature".to_string(), "true".to_string());
        }
        if options.s3_use_environment_credentials {
            values.insert(
                "aws_use_environment_credentials".to_string(),
                "true".to_string(),
            );
        }
        if options.s3_allow_http {
            values.insert("aws_allow_http".to_string(), "true".to_string());
        }
        values.insert(
            "aws_virtual_hosted_style_request".to_string(),
            if options.s3_virtual_hosted_style_request {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        Some(values)
    } else {
        None
    };
    Ok(DatasetOpenOptions {
        uri,
        storage_options,
    })
}

fn build_s3_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<object_store::aws::AmazonS3, String> {
    let mut builder = if storage_options
        .get("aws_use_environment_credentials")
        .is_some_and(|value| value == "true")
    {
        AmazonS3Builder::from_env()
    } else {
        AmazonS3Builder::new()
    }
    .with_url(uri);
    for (key, value) in storage_options {
        if let Ok(config_key) = key.parse::<AmazonS3ConfigKey>() {
            builder = builder.with_config(config_key, value);
        }
    }
    builder.build().map_err(|err| err.to_string())
}

async fn download_s3_dataset(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<TempDir, String> {
    let url = url::Url::parse(uri).map_err(|err| err.to_string())?;
    let prefix = url.path().trim_start_matches('/').trim_end_matches('/');
    if prefix.is_empty() {
        return Err("Lance S3 dataset path must not be empty".to_string());
    }

    let store = build_s3_store(uri, storage_options)?;
    let cache_dir = tempfile::Builder::new()
        .prefix("ch_lance_s3_")
        .tempdir()
        .map_err(|err| err.to_string())?;
    let prefix_path = ObjectStorePath::from(prefix);
    let objects = store
        .list(Some(&prefix_path))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| err.to_string())?;

    for object in objects {
        let object_path = object.location.to_string();
        let relative_path = object_path
            .strip_prefix(prefix)
            .unwrap_or(&object_path)
            .trim_start_matches('/');
        if relative_path.is_empty() {
            continue;
        }

        let local_path: PathBuf = cache_dir.path().join(relative_path);
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| err.to_string())?;
        }
        let bytes = store
            .get(&object.location)
            .await
            .map_err(|err| err.to_string())?
            .bytes()
            .await
            .map_err(|err| err.to_string())?;
        tokio::fs::write(&local_path, bytes)
            .await
            .map_err(|err| err.to_string())?;
    }

    Ok(cache_dir)
}

async fn open_dataset(options: DatasetOpenOptions) -> Result<OpenedDataset, String> {
    if let Some(storage_options) = options.storage_options {
        let cache_dir = download_s3_dataset(&options.uri, &storage_options).await?;
        let dataset = Dataset::open(cache_dir.path().to_string_lossy().as_ref())
            .await
            .map_err(|err| err.to_string())?;
        Ok(OpenedDataset {
            dataset,
            s3_cache: Some(cache_dir),
        })
    } else {
        let dataset = Dataset::open(&options.uri)
            .await
            .map_err(|err| err.to_string())?;
        Ok(OpenedDataset {
            dataset,
            s3_cache: None,
        })
    }
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

    let open_options = match apply_dataset_options(&*options) {
        Ok(options) => options,
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

    match runtime.block_on(open_dataset(open_options)) {
        Ok(opened) => Box::into_raw(Box::new(ch_lance_dataset {
            runtime,
            dataset: opened.dataset,
            _s3_cache: opened.s3_cache,
        })),
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

    fn dataset_options(uri: &CString) -> ch_lance_dataset_options {
        ch_lance_dataset_options {
            uri: uri.as_ptr(),
            use_s3: false,
            s3_region: ptr::null(),
            s3_endpoint: ptr::null(),
            s3_access_key_id: ptr::null(),
            s3_secret_access_key: ptr::null(),
            s3_session_token: ptr::null(),
            s3_role_arn: ptr::null(),
            s3_role_session_name: ptr::null(),
            s3_use_environment_credentials: false,
            s3_no_sign_request: false,
            s3_allow_http: false,
            s3_virtual_hosted_style_request: false,
        }
    }

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
        let options = dataset_options(&uri);
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
        let options = dataset_options(&uri);
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

    #[test]
    fn open_s3_dataset_from_env_when_configured() {
        let Ok(uri) = std::env::var("CH_LANCE_TEST_S3_URI") else {
            return;
        };

        let mut storage_options = HashMap::new();
        for (env_name, option_name) in [
            ("CH_LANCE_TEST_S3_ENDPOINT", "aws_endpoint"),
            ("CH_LANCE_TEST_S3_REGION", "aws_region"),
            ("CH_LANCE_TEST_S3_ACCESS_KEY_ID", "aws_access_key_id"),
            (
                "CH_LANCE_TEST_S3_SECRET_ACCESS_KEY",
                "aws_secret_access_key",
            ),
        ] {
            if let Ok(value) = std::env::var(env_name) {
                storage_options.insert(option_name.to_string(), value);
            }
        }
        storage_options.insert("aws_allow_http".to_string(), "true".to_string());
        storage_options.insert(
            "aws_virtual_hosted_style_request".to_string(),
            "false".to_string(),
        );

        let runtime = Runtime::new().unwrap();
        let opened = runtime
            .block_on(open_dataset(DatasetOpenOptions {
                uri,
                storage_options: Some(storage_options),
            }))
            .unwrap();
        let rows = runtime.block_on(opened.dataset.count_rows(None)).unwrap();
        assert_eq!(rows, 3);

        let scan_rows = runtime.block_on(async {
            let mut stream = opened.dataset.scan().try_into_stream().await.unwrap();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
            rows
        });
        assert_eq!(scan_rows, 3);

        let uri = CString::new(std::env::var("CH_LANCE_TEST_S3_URI").unwrap()).unwrap();
        let endpoint = CString::new(std::env::var("CH_LANCE_TEST_S3_ENDPOINT").unwrap()).unwrap();
        let region = CString::new(
            std::env::var("CH_LANCE_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        )
        .unwrap();
        let access_key =
            CString::new(std::env::var("CH_LANCE_TEST_S3_ACCESS_KEY_ID").unwrap()).unwrap();
        let secret_key =
            CString::new(std::env::var("CH_LANCE_TEST_S3_SECRET_ACCESS_KEY").unwrap()).unwrap();
        let options = ch_lance_dataset_options {
            uri: uri.as_ptr(),
            use_s3: true,
            s3_region: region.as_ptr(),
            s3_endpoint: endpoint.as_ptr(),
            s3_access_key_id: access_key.as_ptr(),
            s3_secret_access_key: secret_key.as_ptr(),
            s3_session_token: ptr::null(),
            s3_role_arn: ptr::null(),
            s3_role_session_name: ptr::null(),
            s3_use_environment_credentials: false,
            s3_no_sign_request: false,
            s3_allow_http: true,
            s3_virtual_hosted_style_request: false,
        };
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
        let projection = [
            CString::new("id").unwrap(),
            CString::new("name").unwrap(),
            CString::new("score").unwrap(),
        ];
        let projection_ptrs = projection
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let scan_options = ch_lance_scan_options {
            snapshot_id: snapshot.snapshot_id,
            projection: ch_lance_string_list {
                values: projection_ptrs.as_ptr(),
                size: projection_ptrs.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 8192,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(!scan.is_null());
        let mut ffi_rows = 0;
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let mut has_batch = false;
            assert!(unsafe {
                ch_lance_next_batch(
                    scan,
                    addr_of_mut!(array),
                    addr_of_mut!(schema),
                    addr_of_mut!(has_batch),
                    addr_of_mut!(error),
                )
            });
            if !has_batch {
                break;
            }
            let struct_data = unsafe { from_ffi(array, &schema) }.unwrap();
            ffi_rows += StructArray::from(make_array(struct_data).to_data()).len();
        }
        assert_eq!(ffi_rows, 3);
        unsafe {
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }
}
