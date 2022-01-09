#include "SharedLibraryHandler.h"

#include <base/scope_guard.h>
#include <base/bit_cast.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>
#include "ClickHouseLibrarySource.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
}


SharedLibraryHandler::SharedLibraryHandler(
    const std::string & library_path_,
    const std::vector<std::string> & library_settings,
    const Block & sample_block_,
    size_t max_block_size_,
    const std::vector<std::string> & attributes_names_)
    : library_path(library_path_)
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , attributes_names(attributes_names_)
{
    library = std::make_shared<SharedLibrary>(library_path, RTLD_LAZY);
    settings_holder = std::make_shared<CStringsHolder>(CStringsHolder(library_settings));

    auto lib_new = library->tryGet<ClickHouseLibrary::LibraryNewFunc>(ClickHouseLibrary::LIBRARY_CREATE_NEW_FUNC_NAME);

    if (lib_new)
        lib_data = lib_new(&settings_holder->strings, ClickHouseLibrary::log);
    else
        throw Exception("Method libNew failed", ErrorCodes::EXTERNAL_LIBRARY_ERROR);
}


SharedLibraryHandler::SharedLibraryHandler(const SharedLibraryHandler & other)
    : library_path{other.library_path}
    , sample_block{other.sample_block}
    , attributes_names{other.attributes_names}
    , library{other.library}
    , settings_holder{other.settings_holder}
{

    auto lib_clone = library->tryGet<ClickHouseLibrary::LibraryCloneFunc>(ClickHouseLibrary::LIBRARY_CLONE_FUNC_NAME);

    if (lib_clone)
    {
        lib_data = lib_clone(other.lib_data);
    }
    else
    {
        auto lib_new = library->tryGet<ClickHouseLibrary::LibraryNewFunc>(ClickHouseLibrary::LIBRARY_CREATE_NEW_FUNC_NAME);

        if (lib_new)
            lib_data = lib_new(&settings_holder->strings, ClickHouseLibrary::log);
    }
}


SharedLibraryHandler::~SharedLibraryHandler()
{
    auto lib_delete = library->tryGet<ClickHouseLibrary::LibraryDeleteFunc>(ClickHouseLibrary::LIBRARY_DELETE_FUNC_NAME);

    if (lib_delete)
        lib_delete(lib_data);
}


bool SharedLibraryHandler::isModified()
{
    auto func_is_modified = library->tryGet<ClickHouseLibrary::LibraryIsModifiedFunc>(ClickHouseLibrary::LIBRARY_IS_MODIFIED_FUNC_NAME);

    if (func_is_modified)
        return func_is_modified(lib_data, &settings_holder->strings);

    return true;
}


bool SharedLibraryHandler::supportsSelectiveLoad()
{
    auto func_supports_selective_load = library->tryGet<ClickHouseLibrary::LibrarySupportsSelectiveLoadFunc>(ClickHouseLibrary::LIBRARY_SUPPORTS_SELECTIVE_LOAD_FUNC_NAME);

    if (func_supports_selective_load)
        return func_supports_selective_load(lib_data, &settings_holder->strings);

    return true;
}


ClickHouseLibrarySourcePtr SharedLibraryHandler::loadAll()
{
    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(attributes_names.size());
    ClickHouseLibrary::CStrings columns{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), attributes_names.size()};
    for (size_t i = 0; i < attributes_names.size(); ++i)
        columns.data[i] = attributes_names[i].c_str();

    auto load_all_func = library->get<ClickHouseLibrary::LibraryLoadAllFunc>(ClickHouseLibrary::LIBRARY_LOAD_ALL_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);

    auto state = std::make_unique<LibraryResultState>();
    state->data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);
    state->data_ptr = data_new_func(lib_data);
    state->data = load_all_func(state->data_ptr, &settings_holder->strings, &columns);

    return std::make_shared<ClickHouseLibrarySource>(std::move(state), sample_block, max_block_size);
}


ClickHouseLibrarySourcePtr SharedLibraryHandler::loadIds(const std::vector<uint64_t> & ids)
{
    const ClickHouseLibrary::VectorUInt64 ids_data{bit_cast<decltype(ClickHouseLibrary::VectorUInt64::data)>(ids.data()), ids.size()};

    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(attributes_names.size());
    ClickHouseLibrary::CStrings columns_pass{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), attributes_names.size()};

    auto load_ids_func = library->get<ClickHouseLibrary::LibraryLoadIdsFunc>(ClickHouseLibrary::LIBRARY_LOAD_IDS_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);

    auto state = std::make_unique<LibraryResultState>();
    state->data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);
    state->data_ptr = data_new_func(lib_data);
    state->data = load_ids_func(state->data_ptr, &settings_holder->strings, &columns_pass, &ids_data);

    return std::make_shared<ClickHouseLibrarySource>(std::move(state), sample_block, max_block_size);
}


ClickHouseLibrarySourcePtr SharedLibraryHandler::loadKeys(const Columns & key_columns)
{
    auto holder = std::make_unique<ClickHouseLibrary::Row[]>(key_columns.size());
    std::vector<std::unique_ptr<ClickHouseLibrary::Field[]>> column_data_holders;

    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        auto cell_holder = std::make_unique<ClickHouseLibrary::Field[]>(key_columns[i]->size());

        for (size_t j = 0; j < key_columns[i]->size(); ++j)
        {
            auto data_ref = key_columns[i]->getDataAt(j);

            cell_holder[j] = ClickHouseLibrary::Field{
                    .data = static_cast<const void *>(data_ref.data),
                    .size = data_ref.size};
        }

        holder[i] = ClickHouseLibrary::Row{
                    .data = static_cast<ClickHouseLibrary::Field *>(cell_holder.get()),
                    .size = key_columns[i]->size()};

        column_data_holders.push_back(std::move(cell_holder));
    }

    ClickHouseLibrary::Table request_cols{
            .data = static_cast<ClickHouseLibrary::Row *>(holder.get()),
            .size = key_columns.size()};

    auto load_keys_func = library->get<ClickHouseLibrary::LibraryLoadKeysFunc>(ClickHouseLibrary::LIBRARY_LOAD_KEYS_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);

    auto state = std::make_unique<LibraryResultState>();
    state->data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);
    state->data_ptr = data_new_func(lib_data);
    state->data = load_keys_func(state->data_ptr, &settings_holder->strings, &request_cols);

    return std::make_shared<ClickHouseLibrarySource>(std::move(state), sample_block, max_block_size);
}

}
