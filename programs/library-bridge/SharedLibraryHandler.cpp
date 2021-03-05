#include "SharedLibraryHandler.h"

#include <boost/algorithm/string/split.hpp>
#include <ext/scope_guard.h>
#include <IO/ReadHelpers.h>


namespace DB
{

SharedLibraryHandler::SharedLibraryHandler(const std::string & dictionary_id_)
  : log(&Poco::Logger::get("SharedLibraryHandler"))
  , dictionary_id(dictionary_id_)
{
}


SharedLibraryHandler::~SharedLibraryHandler()
{
    auto lib_delete = library->tryGet<void (*)(decltype(lib_data))>("ClickHouseDictionary_v3_libDelete");

    if (lib_delete)
        lib_delete(lib_data);
}


void SharedLibraryHandler::libNew(const std::string & path, const std::string & settings)
{
    library_path = path;

    library = std::make_shared<SharedLibrary>(library_path, RTLD_LAZY
#if defined(RTLD_DEEPBIND) && !defined(ADDRESS_SANITIZER) // Does not exists in FreeBSD. Cannot work with Address Sanitizer.
    | RTLD_DEEPBIND
#endif
    );

    std::vector<std::string> lib_settings;
    boost::split(lib_settings, settings, [](char c) { return c == ' '; });
    settings_holder = std::make_shared<CStringsHolder>(CStringsHolder(lib_settings));

    auto lib_new = library->tryGet<decltype(lib_data) (*)(
        decltype(&settings_holder->strings), decltype(&ClickHouseLibrary::log))>("ClickHouseDictionary_v3_libNew");

    if (lib_new)
        lib_data = lib_new(&settings_holder->strings, ClickHouseLibrary::log);
}


//void SharedLibraryHandler::libCloneOrNew()
//{
//    if (auto lib_clone = library->tryGet<decltype(lib_data) (*)(decltype(other.lib_data))>("ClickHouseDictionary_v3_libClone"))
//        lib_data = lib_clone(other.lib_data);
//    else if (auto lib_new = library->tryGet<decltype(lib_data) (*)(decltype(&settings->strings), decltype(&ClickHouseLibrary::log))>("ClickHouseDictionary_v3_libNew"))
//        lib_data = lib_new(&settings->strings, ClickHouseLibrary::log);
//}


BlockInputStreamPtr SharedLibraryHandler::loadAll(const std::string & attributes_string, const Block & sample_block)
{
    std::vector<std::string> dict_attributes;
    boost::split(dict_attributes, attributes_string, [](char c) { return c == ','; });

    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_attributes.size());
    ClickHouseLibrary::CStrings columns{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), dict_attributes.size()};
    size_t i = 0;

    for (const auto & attr : dict_attributes)
        columns.data[i++] = attr.c_str();

    void * data_ptr = nullptr;

    /// Get function pointer before dataNew call because library->get may throw.
    auto func_load_all = library->get<void * (*)(
        decltype(data_ptr), decltype(&settings_holder->strings), decltype(&columns))>("ClickHouseDictionary_v3_loadAll");

    data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    auto * data = func_load_all(data_ptr, &settings_holder->strings, &columns);
    auto block = dataToBlock(sample_block, data);

    SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));

    return std::make_shared<OneBlockInputStream>(block);
}


BlockInputStreamPtr SharedLibraryHandler::loadIds(const std::string & attributes_string, const std::string & ids_string, const Block & sample_block)
{
    std::vector<std::string> dict_string_ids;
    boost::split(dict_string_ids, ids_string, [](char c) { return c == ','; });
    std::vector<UInt64> dict_ids;
    for (const auto & id : dict_string_ids)
        dict_ids.push_back(parseFromString<UInt64>(id));

    std::vector<std::string> dict_attributes;
    boost::split(dict_attributes, attributes_string, [](char c) { return c == ','; });

    const ClickHouseLibrary::VectorUInt64 ids_data{ext::bit_cast<decltype(ClickHouseLibrary::VectorUInt64::data)>(dict_ids.data()), dict_ids.size()};
    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_attributes.size());
    ClickHouseLibrary::CStrings columns_pass{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), dict_attributes.size()};

    void * data_ptr = nullptr;

    /// Get function pointer before dataNew call because library->get may throw.
    auto func_load_ids = library->get<void * (*)(
        decltype(data_ptr), decltype(&settings_holder->strings), decltype(&columns_pass), decltype(&ids_data))>("ClickHouseDictionary_v3_loadIds");

    data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    auto * data = func_load_ids(data_ptr, &settings_holder->strings, &columns_pass, &ids_data);
    auto block = dataToBlock(sample_block, data);

    SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));

    return std::make_shared<OneBlockInputStream>(block);
}


Block SharedLibraryHandler::dataToBlock(const Block & sample_block, const void * data)
{
    if (!data)
        throw Exception("LibraryDictionarySource: No data returned", ErrorCodes::EXTERNAL_LIBRARY_ERROR);

    const auto * columns_received = static_cast<const ClickHouseLibrary::Table *>(data);
    if (columns_received->error_code)
        throw Exception(
            "LibraryDictionarySource: Returned error: " + std::to_string(columns_received->error_code) + " " + (columns_received->error_string ? columns_received->error_string : ""),
            ErrorCodes::EXTERNAL_LIBRARY_ERROR);

    MutableColumns columns(sample_block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = sample_block.getByPosition(i).column->cloneEmpty();

    for (size_t col_n = 0; col_n < columns_received->size; ++col_n)
    {
        if (columns.size() != columns_received->data[col_n].size)
            throw Exception(
                "LibraryDictionarySource: Returned unexpected number of columns: " + std::to_string(columns_received->data[col_n].size) + ", must be " + std::to_string(columns.size()),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        for (size_t row_n = 0; row_n < columns_received->data[col_n].size; ++row_n)
        {
            const auto & field = columns_received->data[col_n].data[row_n];
            if (!field.data)
            {
                /// sample_block contains null_value (from config) inside corresponding column
                const auto & col = sample_block.getByPosition(row_n);
                columns[row_n]->insertFrom(*(col.column), 0);
            }
            else
            {
                const auto & size = field.size;
                columns[row_n]->insertData(static_cast<const char *>(field.data), size);
            }
        }
    }

    return sample_block.cloneWithColumns(std::move(columns));
}

}
