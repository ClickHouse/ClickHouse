#include <cstddef>
#include <Core/SchemaInferenceMode.h>

namespace DB {

struct StorageObjectStorageQuerySettings
{
    /// Insert settings:
    bool truncate_on_insert;
    bool create_new_file_on_insert;

    /// Schema inference settings:
    bool schema_inference_use_cache;
    SchemaInferenceMode schema_inference_mode;

    /// List settings:
    bool skip_empty_files;
    size_t list_object_keys_size;
    bool throw_on_zero_files_match;
    bool ignore_non_existent_file;
};

}
