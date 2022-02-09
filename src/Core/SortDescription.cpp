#include <base/logger_useful.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/SipHash.h>

#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>

namespace DB
{

void dumpSortDescription(const SortDescription & description, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        out << desc.column_name;

        if (desc.direction > 0)
            out << " ASC";
        else
            out << " DESC";

        if (desc.with_fill)
            out << " WITH FILL";
    }
}

void SortColumnDescription::explain(JSONBuilder::JSONMap & map) const
{
    map.add("Column", column_name);
    map.add("Ascending", direction > 0);
    map.add("With Fill", with_fill);
}

#if USE_EMBEDDED_COMPILER

static CHJIT & getJITInstance()
{
    static CHJIT jit;
    return jit;
}

class CompiledSortDescriptionFunctionHolder final : public CompiledExpressionCacheEntry
{
public:
    explicit CompiledSortDescriptionFunctionHolder(CompiledSortDescriptionFunction compiled_function_)
        : CompiledExpressionCacheEntry(compiled_function_.compiled_module.size)
        , compiled_sort_description_function(compiled_function_)
    {}

    ~CompiledSortDescriptionFunctionHolder() override
    {
        getJITInstance().deleteCompiledModule(compiled_sort_description_function.compiled_module);
    }

    CompiledSortDescriptionFunction compiled_sort_description_function;
};

static std::unordered_map<std::string, CompiledSortDescriptionFunction> sort_description_to_compiled_function;

static std::string getSortDescriptionDump(const SortDescription & description, const DataTypes & description_types)
{
    WriteBufferFromOwnString buffer;

    for (size_t i = 0; i < description.size(); ++i)
        buffer << description_types[i]->getName() << ' ' << description[i].direction << ' ' << description[i].nulls_direction;

    return buffer.str();
}

static Poco::Logger * getLogger()
{
    static Poco::Logger & logger = Poco::Logger::get("SortDescription");
    return &logger;
}

void compileSortDescriptionIfNeeded(SortDescription & description, const DataTypes & description_types)
{
    for (const auto & type : description_types)
    {
        if (!type->createColumn()->isComparatorCompilable())
            return;
    }

    auto description_dump = getSortDescriptionDump(description, description_types);

    SipHash sort_description_dump_hash;
    sort_description_dump_hash.update(description_dump);

    UInt128 aggregate_functions_description_hash_key;
    sort_description_dump_hash.get128(aggregate_functions_description_hash_key);

    std::shared_ptr<CompiledSortDescriptionFunctionHolder> compiled_sort_description_holder;

    if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        auto [compiled_function_cache_entry, _] = compilation_cache->getOrSet(aggregate_functions_description_hash_key, [&] ()
        {
            LOG_TRACE(getLogger(), "Compile sort description {}", description_dump);

            auto compiled_sort_description = compileSortDescription(getJITInstance(), description, description_types, description_dump);
            return std::make_shared<CompiledSortDescriptionFunctionHolder>(std::move(compiled_sort_description));
        });

        compiled_sort_description_holder = std::static_pointer_cast<CompiledSortDescriptionFunctionHolder>(compiled_function_cache_entry);
    }
    else
    {
        LOG_TRACE(getLogger(), "Compile sort description {}", description_dump);
        auto compiled_sort_description = compileSortDescription(getJITInstance(), description, description_types, description_dump);
        compiled_sort_description_holder = std::make_shared<CompiledSortDescriptionFunctionHolder>(std::move(compiled_sort_description));
    }

    auto comparator_function = compiled_sort_description_holder->compiled_sort_description_function.comparator_function;
    description.compiled_sort_description = reinterpret_cast<void *>(comparator_function);
    description.compiled_sort_description_holder = std::move(compiled_sort_description_holder);
}

#else

void compileSortDescriptionIfNeeded(SortDescription & description, const DataTypes & description_types)
{
    (void)(description);
    (void)(description_types);
}

#endif

std::string dumpSortDescription(const SortDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpSortDescription(description, wb);
    return wb.str();
}

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description)
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & descr : description)
    {
        auto json_map = std::make_unique<JSONBuilder::JSONMap>();
        descr.explain(*json_map);
        json_array->add(std::move(json_map));
    }

    return json_array;
}

}
