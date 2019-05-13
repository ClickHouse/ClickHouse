#include <Storages/StorageJoin.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/NestedUtils.h>

#include <Poco/String.h>    /// toLower
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SET_DATA_VARIANT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StorageJoin::StorageJoin(
    const String & path_,
    const String & name_,
    const Names & key_names_,
    bool use_nulls_,
    SizeLimits limits_,
    ASTTableJoin::Kind kind_,
    ASTTableJoin::Strictness strictness_,
    const ColumnsDescription & columns_,
    bool overwrite)
    : StorageSetOrJoinBase{path_, name_, columns_}
    , key_names(key_names_)
    , use_nulls(use_nulls_)
    , limits(limits_)
    , kind(kind_)
    , strictness(strictness_)
{
    for (const auto & key : key_names)
        if (!getColumns().hasPhysical(key))
            throw Exception{"Key column (" + key + ") does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

    join = std::make_shared<Join>(key_names, use_nulls, limits, kind, strictness, overwrite);
    join->setSampleBlock(getSampleBlock().sortColumns());
    restore();
}


void StorageJoin::truncate(const ASTPtr &, const Context &)
{
    Poco::File(path).remove(true);
    Poco::File(path).createDirectories();
    Poco::File(path + "tmp/").createDirectories();

    increment = 0;
    join = std::make_shared<Join>(key_names, use_nulls, limits, kind, strictness);
    join->setSampleBlock(getSampleBlock().sortColumns());
}


void StorageJoin::assertCompatible(ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_) const
{
    /// NOTE Could be more loose.
    if (!(kind == kind_ && strictness == strictness_))
        throw Exception("Table " + table_name + " has incompatible type of JOIN.", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);
}


void StorageJoin::insertBlock(const Block & block) { join->insertFromBlock(block); }
size_t StorageJoin::getSize() const { return join->getTotalRowCount(); }


void registerStorageJoin(StorageFactory & factory)
{
    factory.registerStorage("Join", [](const StorageFactory::Arguments & args)
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 3)
            throw Exception(
                "Storage Join requires at least 3 parameters: Join(ANY|ALL, LEFT|INNER, keys...).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto opt_strictness_id = getIdentifierName(engine_args[0]);
        if (!opt_strictness_id)
            throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        const String strictness_str = Poco::toLower(*opt_strictness_id);
        ASTTableJoin::Strictness strictness;
        if (strictness_str == "any")
            strictness = ASTTableJoin::Strictness::Any;
        else if (strictness_str == "all")
            strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        auto opt_kind_id = getIdentifierName(engine_args[1]);
        if (!opt_kind_id)
            throw Exception("Second parameter of storage Join must be LEFT or INNER (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        const String kind_str = Poco::toLower(*opt_kind_id);
        ASTTableJoin::Kind kind;
        if (kind_str == "left")
            kind = ASTTableJoin::Kind::Left;
        else if (kind_str == "inner")
            kind = ASTTableJoin::Kind::Inner;
        else if (kind_str == "right")
            kind = ASTTableJoin::Kind::Right;
        else if (kind_str == "full")
            kind = ASTTableJoin::Kind::Full;
        else
            throw Exception("Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        Names key_names;
        key_names.reserve(engine_args.size() - 2);
        for (size_t i = 2, size = engine_args.size(); i < size; ++i)
        {
            auto opt_key = getIdentifierName(engine_args[i]);
            if (!opt_key)
                throw Exception("Parameter №" + toString(i + 1) + " of storage Join don't look like column name.", ErrorCodes::BAD_ARGUMENTS);

            key_names.push_back(*opt_key);
        }

        auto & settings = args.context.getSettingsRef();
        auto join_use_nulls = settings.join_use_nulls;
        auto max_rows_in_join = settings.max_rows_in_join;
        auto max_bytes_in_join = settings.max_bytes_in_join;
        auto join_overflow_mode = settings.join_overflow_mode;
        auto join_any_take_last_row = settings.join_any_take_last_row;

        if (args.storage_def && args.storage_def->settings)
        {
            for (const auto & setting : args.storage_def->settings->changes)
            {
                if (setting.name == "join_use_nulls")
                    join_use_nulls.set(setting.value);
                else if (setting.name == "max_rows_in_join")
                    max_rows_in_join.set(setting.value);
                else if (setting.name == "max_bytes_in_join")
                    max_bytes_in_join.set(setting.value);
                else if (setting.name == "join_overflow_mode")
                    join_overflow_mode.set(setting.value);
                else if (setting.name == "join_any_take_last_row")
                    join_any_take_last_row.set(setting.value);
                else
                    throw Exception(
                        "Unknown setting " + setting.name + " for storage " + args.engine_name,
                        ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageJoin::create(
            args.data_path,
            args.table_name,
            key_names,
            join_use_nulls.value,
            SizeLimits{max_rows_in_join.value, max_bytes_in_join.value, join_overflow_mode.value},
            kind,
            strictness,
            args.columns,
            join_any_take_last_row);
    });
}

template <typename T>
static const char * rawData(T & t)
{
    return reinterpret_cast<const char *>(&t);
}
template <typename T>
static size_t rawSize(T &)
{
    return sizeof(T);
}
template <>
const char * rawData(const StringRef & t)
{
    return t.data;
}
template <>
size_t rawSize(const StringRef & t)
{
    return t.size;
}

class JoinBlockInputStream : public IBlockInputStream
{
public:
    JoinBlockInputStream(const Join & parent_, UInt64 max_block_size_, Block && sample_block_)
        : parent(parent_), lock(parent.rwlock), max_block_size(max_block_size_), sample_block(std::move(sample_block_))
    {
        columns.resize(sample_block.columns());
        column_indices.resize(sample_block.columns());
        column_with_null.resize(sample_block.columns());
        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            auto & [_, type, name] = sample_block.getByPosition(i);
            if (parent.sample_block_with_keys.has(name))
            {
                key_pos = i;
                column_with_null[i] = parent.sample_block_with_keys.getByName(name).type->isNullable();
            }
            else
            {
                auto pos = parent.sample_block_with_columns_to_add.getPositionByName(name);
                column_indices[i] = pos;
                column_with_null[i] = !parent.sample_block_with_columns_to_add.getByPosition(pos).type->equals(*type);
            }
        }
    }

    String getName() const override { return "Join"; }

    Block getHeader() const override { return sample_block; }


protected:
    Block readImpl() override
    {
        if (parent.blocks.empty())
            return Block();

        Block block;
        if (parent.dispatch([&](auto, auto strictness, auto & map) { block = createBlock<strictness>(map); }))
            ;
        else
            throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
        return block;
    }

private:
    const Join & parent;
    std::shared_lock<std::shared_mutex> lock;
    UInt64 max_block_size;
    Block sample_block;

    ColumnNumbers column_indices;
    std::vector<bool> column_with_null;
    std::optional<size_t> key_pos;
    MutableColumns columns;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Block createBlock(const Maps & maps)
    {
        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            const auto & src_col = sample_block.safeGetByPosition(i);
            columns[i] = src_col.type->createColumn();
            if (column_with_null[i])
            {
                if (key_pos == i)
                {
                    // unwrap null key column
                    ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*columns[i]);
                    columns[i] = nullable_col.getNestedColumnPtr()->assumeMutable();
                }
                else
                    // wrap non key column with null
                    columns[i] = makeNullable(std::move(columns[i]))->assumeMutable();
            }
        }

        size_t rows_added = 0;

        switch (parent.type)
        {
#define M(TYPE)                                           \
    case Join::Type::TYPE:                                \
        rows_added = fillColumns<STRICTNESS>(*maps.TYPE); \
        break;
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throw Exception("Unknown JOIN keys variant for limited use", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }

        if (!rows_added)
            return {};

        Block res = sample_block.cloneEmpty();
        for (size_t i = 0; i < columns.size(); ++i)
            if (column_with_null[i])
            {
                if (key_pos == i)
                    res.getByPosition(i).column = makeNullable(std::move(columns[i]));
                else
                {
                    const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*columns[i]);
                    res.getByPosition(i).column = nullable_col.getNestedColumnPtr();
                }
            }
            else
                res.getByPosition(i).column = std::move(columns[i]);

        return res;
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())),
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                for (size_t j = 0; j < columns.size(); ++j)
                    if (j == key_pos)
                        columns[j]->insertData(rawData(it->getFirst()), rawSize(it->getFirst()));
                    else
                        columns[j]->insertFrom(*it->getSecond().block->getByPosition(column_indices[j]).column.get(), it->getSecond().row_num);
                ++rows_added;
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
            {
                throw Exception("ASOF join storage is not implemented yet", ErrorCodes::NOT_IMPLEMENTED);
            }
            else
                for (auto current = &static_cast<const typename Map::mapped_type::Base &>(it->getSecond()); current != nullptr;
                     current = current->next)
                {
                    for (size_t j = 0; j < columns.size(); ++j)
                        if (j == key_pos)
                            columns[j]->insertData(rawData(it->getFirst()), rawSize(it->getFirst()));
                        else
                            columns[j]->insertFrom(*current->block->getByPosition(column_indices[j]).column.get(), current->row_num);
                    ++rows_added;
                }

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }
};


// TODO: multiple stream read and index read
BlockInputStreams StorageJoin::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    check(column_names);
    return {std::make_shared<JoinBlockInputStream>(*join, max_block_size, getSampleBlockForColumns(column_names))};
}

}
