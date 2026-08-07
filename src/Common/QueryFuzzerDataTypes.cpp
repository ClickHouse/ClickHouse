#include <Common/QueryFuzzer.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <base/EnumReflection.h>

/// Data-type fuzzing for QueryFuzzer: fuzzDataType and getRandomType with their helpers.
/// Split out of QueryFuzzer.cpp so the type machinery is reviewable on its own.

namespace DB
{

bool QueryFuzzer::fuzzDataTypes(DataTypes & types)
{
    bool changed = false;
    for (auto & elem : types)
    {
        auto fuzzed = fuzzDataType(elem);
        if (fuzzed != elem)
        {
            elem = fuzzed;
            changed = true;
        }
    }
    return changed;
}

const std::unordered_set<String> & QueryFuzzer::geoAliasNames()
{
    /// Geometry is registered as a Variant over every geo alias, so its variants are the authoritative set.
    static const std::unordered_set<String> names = []
    {
        std::unordered_set<String> result{"Geometry"};
        const auto geometry = DataTypeFactory::instance().get("Geometry");
        if (const auto * variant = typeid_cast<const DataTypeVariant *>(geometry.get()))
            for (const auto & alternative : variant->getVariants())
                result.insert(alternative->getName());
        return result;
    }();
    return names;
}

DataTypePtr QueryFuzzer::fuzzContainerChildren(const DataTypePtr & type)
{
    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(fuzzDataType(type_array->getNestedType()));
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = type_tuple->getElements();
        fuzzDataTypes(elements);
        return std::make_shared<DataTypeTuple>(elements);
    }
    if (const auto * type_variant = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        DataTypes variants = type_variant->getVariants();
        fuzzDataTypes(variants);
        return std::make_shared<DataTypeVariant>(variants);
    }
    return nullptr;
}

bool QueryFuzzer::fuzzAggregateName(String & name, size_t nargs)
{
    /// makeAggregateFunctionType re-validates the candidate and falls back if the factory rejects it.
    const auto & swap_aggrs = swapAggregateNames();
    if (nargs > 0 && nargs < 3 && swap_aggrs.contains(nargs) && fuzz_rand() % 3 == 0)
    {
        String candidate = pickRandomly(fuzz_rand, swap_aggrs.at(nargs));
        if (candidate != name)
        {
            name = std::move(candidate);
            return true;
        }
    }
    return false;
}

bool QueryFuzzer::fuzzAggregateParameters(Array & parameters)
{
    /// The factory re-validates and rejects unusable fuzzed parameters.
    if (!parameters.empty() && fuzz_rand() % 3 == 0)
    {
        for (auto & param : parameters)
            param = fuzzField(param);
        return true;
    }
    return false;
}

DataTypePtr QueryFuzzer::fuzzDataType(DataTypePtr type)
{
    checkIterationLimit();

    /// A custom-named type (SimpleAggregateFunction, Nested, geo aliases, Bool) has a plain storage type, so it
    /// must be handled before the structural arms below: those match the storage type and would strip the name.
    if (type->hasCustomName())
    {
        /// SimpleAggregateFunction: fuzz name (candidate set)/args/params, re-validate via the factory.
        if (const auto * type_simple_aggr
            = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
        {
            if (fuzz_rand() % 4 != 0)
            {
                String new_name = type_simple_aggr->getFunctionName();
                DataTypes new_arg_types = type_simple_aggr->getArgumentsDataTypes();
                Array new_parameters = type_simple_aggr->getParameters();
                bool changed = fuzzAggregateName(new_name, new_arg_types.size());
                changed |= fuzzDataTypes(new_arg_types);
                changed |= fuzzAggregateParameters(new_parameters);
                if (changed)
                {
                    if (auto fuzzed = makeAggregateFunctionType(
                            new_name, new_arg_types, new_parameters, /*simple=*/true))
                        return fuzzed;
                }
            }
            return type;
        }

        /// Nested(a T1, b T2, ...) is an Array(Tuple(...)) alias: fuzz each element, keep the names.
        if (const auto * type_nested = typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()))
        {
            if (fuzz_rand() % 4 != 0)
            {
                DataTypes new_elems = type_nested->getElements();
                fuzzDataTypes(new_elems);
                return createNested(new_elems, type_nested->getNames());
            }
            return type;
        }

        /// A geo alias emits its own name regardless of the storage, so a mutated storage cannot round-trip
        /// under it. Occasionally emit the fuzzed storage type instead, dropping the alias.
        if (geoAliasNames().contains(type->getCustomName()->getName()) && fuzz_rand() % 4 == 0)
        {
            try
            {
                if (auto fuzzed = fuzzContainerChildren(type))
                    return fuzzed;
            }
            catch (...) // NOLINT(bugprone-empty-catch) Ok: a fuzzed storage type may violate a container invariant
            {
                return type;
            }
        }

        /// Any other custom-named type (Bool, ...) is a leaf alias with no children to fuzz. Go straight to the
        /// wrapping/replacement tail: returning the type here would freeze it for every seed.
        return fuzzTypeWrapping(type);
    }

    /// Do not replace Array/Tuple/etc. with not Array/Tuple too often.
    const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
    if (type_array && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeArray>(fuzzDataType(type_array->getNestedType()));

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
    if (type_tuple && fuzz_rand() % 4 != 0)
    {
        DataTypes elements = type_tuple->getElements();
        fuzzDataTypes(elements);
        /// Occasionally add a new alternative
        if (elements.size() < 10 && fuzz_rand() % 10 == 0)
            elements.push_back(getRandomType());
        /// Occasionally drop an alternative (keep at least 1)
        if (elements.size() > 1 && fuzz_rand() % 10 == 0)
            elements.erase(elements.begin() + fuzz_rand() % elements.size());
        if (type_tuple->hasExplicitNames())
        {
            auto names = type_tuple->getElementNames();
            /// Pad with synthetic names if a field was added, truncate if one was dropped
            while (names.size() < elements.size())
                names.push_back("f" + std::to_string(names.size()));
            names.resize(elements.size());
            return std::make_shared<DataTypeTuple>(elements, names);
        }
        return std::make_shared<DataTypeTuple>(elements);
    }

    const auto * type_map = typeid_cast<const DataTypeMap *>(type.get());
    if (type_map && fuzz_rand() % 4 != 0)
    {
        auto key_type = fuzzDataType(type_map->getKeyType());
        auto value_type = fuzzDataType(type_map->getValueType());
        if (!DataTypeMap::isValidKeyType(key_type))
            key_type = type_map->getKeyType();

        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get());
    if (type_nullable)
    {
        size_t tmp = fuzz_rand() % 3;
        if (tmp == 0)
            return fuzzDataType(type_nullable->getNestedType());

        if (tmp == 1)
        {
            auto nested_type = fuzzDataType(type_nullable->getNestedType());
            if (nested_type->canBeInsideNullable())
                return std::make_shared<DataTypeNullable>(nested_type);
        }
    }

    const auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get());
    if (type_low_cardinality)
    {
        size_t tmp = fuzz_rand() % 3;
        if (tmp == 0)
            return fuzzDataType(type_low_cardinality->getDictionaryType());

        if (tmp == 1)
        {
            auto nested_type = fuzzDataType(type_low_cardinality->getDictionaryType());
            if (nested_type->canBeInsideLowCardinality())
                return std::make_shared<DataTypeLowCardinality>(nested_type);
        }
    }

    const auto * type_fixed_string = typeid_cast<const DataTypeFixedString *>(type.get());
    if (type_fixed_string && fuzz_rand() % 4 != 0)
    {
        /// Mutate length by ±2 (relative) or pick a fresh random size
        const size_t n = type_fixed_string->getN();
        const size_t new_n = (fuzz_rand() % 4 == 0)
            ? (fuzz_rand() % MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS + 1)
            : std::clamp<size_t>(
                  std::max<ssize_t>(1, static_cast<ssize_t>(n) + static_cast<ssize_t>(fuzz_rand() % 5) - 2),
                  1,
                  MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS);
        return std::make_shared<DataTypeFixedString>(new_n);
    }

    const auto * type_datetime = typeid_cast<const DataTypeDateTime *>(type.get());
    if (type_datetime && fuzz_rand() % 4 != 0)
        return makeRandomDateTime(); /// keep it default or fuzz to another valid explicit timezone

    const auto * type_datetime64 = typeid_cast<const DataTypeDateTime64 *>(type.get());
    if (type_datetime64 && fuzz_rand() % 4 != 0)
        return makeRandomDateTime64(fuzz_rand() % 10); /// scale in [0, 9], keep/fuzz the explicit timezone

    const auto * type_time64 = typeid_cast<const DataTypeTime64 *>(type.get());
    if (type_time64 && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeTime64>(fuzz_rand() % 10); /// scale in [0, 9]

    const auto * type_dynamic = typeid_cast<const DataTypeDynamic *>(type.get());
    if (type_dynamic && fuzz_rand() % 4 != 0)
        return std::make_shared<DataTypeDynamic>(fuzz_rand() % 255);

    const auto * type_variant = typeid_cast<const DataTypeVariant *>(type.get());
    if (type_variant && fuzz_rand() % 4 != 0)
    {
        DataTypes variants = type_variant->getVariants();
        fuzzDataTypes(variants);
        /// Occasionally add a new alternative
        if (variants.size() < 10 && fuzz_rand() % 4 == 0)
            variants.push_back(getRandomType());
        /// Occasionally drop an alternative (keep at least 1)
        if (variants.size() > 1 && fuzz_rand() % 4 == 0)
            variants.erase(variants.begin() + fuzz_rand() % variants.size());
        return std::make_shared<DataTypeVariant>(variants);
    }

    const auto * type_object = typeid_cast<const DataTypeObject *>(type.get());
    if (type_object && fuzz_rand() % 4 != 0)
    {
        std::unordered_map<String, DataTypePtr> typed_paths = type_object->getTypedPaths();
        for (auto & [path, path_type] : typed_paths)
            path_type = fuzzDataType(path_type);
        try
        {
            return makeRandomObject(
                std::move(typed_paths),
                fuzzObjectPathsToSkip(type_object->getPathsToSkip()),
                fuzzObjectPathRegexpsToSkip(type_object->getPathRegexpsToSkip()),
                type_object->getMaxDynamicPaths(),
                type_object->getMaxDynamicTypes());
        }
        catch (...) // NOLINT(bugprone-empty-catch) Ok: a fuzzed typed-path type may violate an Object invariant
        {
            return type;
        }
    }

    const auto * type_qbit = typeid_cast<const DataTypeQBit *>(type.get());
    if (type_qbit && fuzz_rand() % 4 != 0)
        return makeRandomQBit(); /// Replacing with a fresh valid QBit is a valid, round-trippable mutation.

    /// NOLINTBEGIN(bugprone-macro-parentheses)
    /// Enum types: add or remove enum values
#define FUZZ_ENUM(INT_TYPE) \
    if (const auto * dt_enum = typeid_cast<const DataTypeEnum<INT_TYPE> *>(type.get()); dt_enum && fuzz_rand() % 4 != 0) \
    { \
        auto values = dt_enum->getValues(); \
        if (values.size() < 50 && fuzz_rand() % 3 == 0) \
        { \
            const auto new_val = static_cast<INT_TYPE>(fuzz_rand()); \
            if (!dt_enum->hasValue(new_val)) \
                values.emplace_back("e" + std::to_string(values.size()), new_val); \
        } \
        if (values.size() > 1 && fuzz_rand() % 3 == 0) \
            values.erase(values.begin() + fuzz_rand() % values.size()); \
        if (!values.empty()) \
            return std::make_shared<DataTypeEnum<INT_TYPE>>(values); \
    }
    FUZZ_ENUM(Int8)
    FUZZ_ENUM(Int16)
#undef FUZZ_ENUM

    /// Decimal types: mutate scale, and occasionally the precision tier too
#define FUZZ_DECIMAL(DT) \
    if (const auto * dt_dec = typeid_cast<const DataTypeDecimal<DT> *>(type.get()); dt_dec && fuzz_rand() % 4 != 0) \
    { \
        const UInt32 max_prec = DataTypeDecimal<DT>::maxPrecision(); \
        const UInt32 new_prec = (fuzz_rand() % 4 == 0) ? UInt32(fuzz_rand() % max_prec + 1) : dt_dec->getPrecision(); \
        return std::make_shared<DataTypeDecimal<DT>>(new_prec, UInt32(fuzz_rand() % (new_prec + 1))); \
    }
    FUZZ_DECIMAL(Decimal32)
    FUZZ_DECIMAL(Decimal64)
    FUZZ_DECIMAL(Decimal128)
    FUZZ_DECIMAL(Decimal256)
#undef FUZZ_DECIMAL
    /// NOLINTEND(bugprone-macro-parentheses)

    const auto * type_aggr = typeid_cast<const DataTypeAggregateFunction *>(type.get());
    if (type_aggr && fuzz_rand() % 4 != 0)
    {
        String new_name = type_aggr->getFunctionName();
        DataTypes new_arg_types = type_aggr->getArgumentsDataTypes();
        Array new_parameters = type_aggr->getParameters();
        bool name_changed = fuzzAggregateName(new_name, new_arg_types.size());
        bool changed = name_changed;
        changed |= fuzzDataTypes(new_arg_types);
        changed |= fuzzAggregateParameters(new_parameters);
        if (changed)
        {
            /// Keep the source serialization version only while the name is unchanged: a version valid for the
            /// old function may be out of range for the new one, so a rename drops back to the default.
            std::optional<size_t> version = name_changed ? std::nullopt : type_aggr->getVersionIfExplicit();
            if (auto fuzzed = makeAggregateFunctionType(new_name, new_arg_types, new_parameters, /*simple=*/false, version))
                return fuzzed;
        }
        return type;
    }

    return fuzzTypeWrapping(type);
}

DataTypePtr QueryFuzzer::fuzzTypeWrapping(const DataTypePtr & type)
{
    /// Wrap the type, or replace it with a random one. Wrapping keeps a custom name as the child.
    size_t tmp = fuzz_rand() % 8;
    if (tmp == 0)
        return std::make_shared<DataTypeArray>(type);

    if (tmp <= 1 && type->canBeInsideNullable())
        return std::make_shared<DataTypeNullable>(type);

    if (tmp <= 2 && type->canBeInsideLowCardinality())
        return std::make_shared<DataTypeLowCardinality>(type);

    if (tmp <= 3)
        return getRandomType();

    return type;
}

DataTypePtr QueryFuzzer::makeRandomQBit()
{
    /// QBit only accepts Int8/BFloat16/Float32/Float64 element types and a (dimension, stride) pair
    /// where dimension % stride == 0 and, when actually strided, stride % 8 == 0.
    static const DataTypePtr qbit_element_types[]
        = {std::make_shared<DataTypeInt8>(),
           std::make_shared<DataTypeBFloat16>(),
           std::make_shared<DataTypeFloat32>(),
           std::make_shared<DataTypeFloat64>()};
    const auto & element_type = qbit_element_types[fuzz_rand() % std::size(qbit_element_types)];

    /// Occasionally build a strided QBit, otherwise keep it non-strided (stride == dimension).
    if (fuzz_rand() % 4 == 0)
    {
        const size_t stride = (fuzz_rand() % 16 + 1) * 8; /// multiple of 8 in [8, 128]
        const size_t num_groups = fuzz_rand() % 8 + 1; /// 1..8 stride groups
        const size_t dimension = stride * num_groups;
        return std::make_shared<DataTypeQBit>(element_type, dimension, stride);
    }

    const size_t dimension = fuzz_rand() % 128 + 1;
    return std::make_shared<DataTypeQBit>(element_type, dimension, dimension);
}

std::unordered_set<String> QueryFuzzer::fuzzObjectPathsToSkip(std::unordered_set<String> paths_to_skip)
{
    if (paths_to_skip.empty() || fuzz_rand() % 4 != 0)
        return paths_to_skip;

    /// A skipped path is a compound identifier, so only identifier-shaped values keep the type parseable.
    static constexpr const char * path_names[] = {"a", "b", "c", "a.b", "a.b.c", "SKIP", "x_1", "some.path"};
    static constexpr size_t n_paths = std::size(path_names);

    std::vector<String> paths(paths_to_skip.begin(), paths_to_skip.end());
    std::sort(paths.begin(), paths.end()); /// Iteration order of the set is unspecified; keep it seed-stable.
    paths[fuzz_rand() % paths.size()] = path_names[fuzz_rand() % n_paths];
    if (paths.size() < 4 && fuzz_rand() % 3 == 0)
        paths.push_back(path_names[fuzz_rand() % n_paths]);
    else if (paths.size() > 1 && fuzz_rand() % 3 == 0)
        paths.erase(paths.begin() + fuzz_rand() % paths.size());
    return std::unordered_set<String>(paths.begin(), paths.end());
}

std::vector<String> QueryFuzzer::fuzzObjectPathRegexpsToSkip(std::vector<String> path_regexps_to_skip)
{
    if (path_regexps_to_skip.empty() || fuzz_rand() % 4 != 0)
        return path_regexps_to_skip;

    /// The DataTypeObject constructor rejects a regexp RE2 cannot compile, so only compilable ones are used.
    static constexpr const char * regexps[]
        = {"^a.*$", ".*", "a|b", "[0-9]+", "^$", "(a)(b)?", "\\d{1,3}", "a{2,}", "[[:alpha:]]+", "x(?:y|z)"};
    static constexpr size_t n_regexps = std::size(regexps);

    path_regexps_to_skip[fuzz_rand() % path_regexps_to_skip.size()] = regexps[fuzz_rand() % n_regexps];
    if (path_regexps_to_skip.size() < 4 && fuzz_rand() % 3 == 0)
        path_regexps_to_skip.push_back(regexps[fuzz_rand() % n_regexps]);
    else if (path_regexps_to_skip.size() > 1 && fuzz_rand() % 3 == 0)
        path_regexps_to_skip.erase(path_regexps_to_skip.begin() + fuzz_rand() % path_regexps_to_skip.size());
    return path_regexps_to_skip;
}

DataTypePtr QueryFuzzer::makeRandomObject(
    std::unordered_map<String, DataTypePtr> typed_paths,
    std::unordered_set<String> paths_to_skip,
    std::vector<String> path_regexps_to_skip,
    std::optional<size_t> source_max_dynamic_paths,
    std::optional<size_t> source_max_dynamic_types)
{
    /// Only the numeric parameters are randomized here; the typed paths and SKIP lists are used as given.
    /// An unfired roll keeps the source limit, so mutating one part of the type does not reset the others.
    const size_t max_dynamic_paths = (fuzz_rand() % 4 == 0)
        ? fuzz_rand() % (DataTypeObject::MAX_DYNAMIC_PATHS_LIMIT + 1)
        : source_max_dynamic_paths.value_or(DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS);
    const size_t max_dynamic_types = (fuzz_rand() % 4 == 0)
        ? fuzz_rand() % (ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT + 1)
        : source_max_dynamic_types.value_or(DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES);
    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::move(typed_paths),
        std::move(paths_to_skip),
        std::move(path_regexps_to_skip),
        max_dynamic_paths,
        max_dynamic_types);
}

DataTypePtr QueryFuzzer::makeAggregateFunctionType(
    const String & name, const DataTypes & argument_types, const Array & parameters, bool simple, std::optional<size_t> version)
{
    try
    {
        AggregateFunctionProperties properties;
        auto func = AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, argument_types, parameters, properties);
        DataTypePtr result;
        if (simple)
        {
            DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(func);
            result = createSimpleAggregateFunctionType(func, argument_types, parameters);
        }
        else
        {
            /// Preserve the source serialization version (AggregateFunction(1, ...)); empty keeps the default.
            result = std::make_shared<DataTypeAggregateFunction>(func, argument_types, parameters, version);
        }
        /// The factory accepting the parameters is not enough: a Decimal or big-integer parameter is formatted
        /// as a quoted literal, so the emitted name no longer reparses. Decline when it does not.
        if (!DataTypeFactory::instance().tryGet(result->getName()))
            return nullptr;
        return result;
    }
    catch (...) // NOLINT(bugprone-empty-catch) Ok: the aggregate may reject the given argument types
    {
        return nullptr;
    }
}

namespace
{
    /// Valid timezone names the fuzzer may attach to a DateTime / DateTime64.
    constexpr const char * fuzzer_timezones[] = {"UTC", "Europe/Moscow", "America/New_York", "Asia/Tokyo", "Australia/Sydney"};
}

DataTypePtr QueryFuzzer::makeRandomDateTime()
{
    /// One third of the time attach an explicit valid timezone, otherwise leave it default.
    if (fuzz_rand() % 3 == 0)
        return std::make_shared<DataTypeDateTime>(fuzzer_timezones[fuzz_rand() % std::size(fuzzer_timezones)]);
    return std::make_shared<DataTypeDateTime>();
}

DataTypePtr QueryFuzzer::makeRandomDateTime64(UInt32 scale)
{
    if (fuzz_rand() % 3 == 0)
        return std::make_shared<DataTypeDateTime64>(scale, fuzzer_timezones[fuzz_rand() % std::size(fuzzer_timezones)]);
    return std::make_shared<DataTypeDateTime64>(scale);
}

DataTypePtr QueryFuzzer::getRandomType()
{
    checkIterationLimit();

    static const std::vector<TypeIndex> random_types = {TypeIndex::UInt8,      TypeIndex::UInt16,         TypeIndex::UInt32,
                                                        TypeIndex::UInt64,     TypeIndex::UInt128,        TypeIndex::UInt256,
                                                        TypeIndex::Int8,       TypeIndex::Int16,          TypeIndex::Int32,
                                                        TypeIndex::Int64,      TypeIndex::Int128,         TypeIndex::Int256,
                                                        TypeIndex::BFloat16,   TypeIndex::Float32,        TypeIndex::Float64,
                                                        TypeIndex::Date,       TypeIndex::Date32,         TypeIndex::DateTime,
                                                        TypeIndex::DateTime64, TypeIndex::String,         TypeIndex::FixedString,
                                                        TypeIndex::Enum8,      TypeIndex::Enum16,         TypeIndex::Decimal32,
                                                        TypeIndex::Decimal64,  TypeIndex::Decimal128,     TypeIndex::Decimal256,
                                                        TypeIndex::UUID,       TypeIndex::Array,          TypeIndex::Tuple,
                                                        TypeIndex::Nullable,   TypeIndex::LowCardinality, TypeIndex::Map,
                                                        TypeIndex::IPv4,       TypeIndex::IPv6,           TypeIndex::Variant,
                                                        TypeIndex::Dynamic,    TypeIndex::Time,           TypeIndex::Time64,
                                                        TypeIndex::Object,     TypeIndex::QBit,           TypeIndex::AggregateFunction};

    /// Geo types are custom-named Array aliases with no TypeIndex of their own,
    /// so they are appended after the TypeIndex vector in a unified selection.
    static constexpr const char * geo_type_names[]
        = {"Point", "MultiPoint", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon"};
    static constexpr size_t n_geo = std::size(geo_type_names);
    const size_t pick = fuzz_rand() % (random_types.size() + n_geo);
    if (pick >= random_types.size())
        return DataTypeFactory::instance().get(geo_type_names[pick - random_types.size()]);

    const auto type_id = random_types[pick];

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DISPATCH(DECIMAL) \
    case TypeIndex::DECIMAL: \
        return std::make_shared<DataTypeDecimal<DECIMAL>>( \
            DataTypeDecimal<DECIMAL>::maxPrecision(), (fuzz_rand() % DataTypeDecimal<DECIMAL>::maxPrecision()) + 1);

    switch (type_id)
    {
        case TypeIndex::Tuple: {
            const size_t tuple_size = fuzz_rand() % 6;
            DataTypes elements;
            for (size_t i = 0; i < tuple_size; ++i)
                elements.push_back(getRandomType());
            return std::make_shared<DataTypeTuple>(elements);
        }
        case TypeIndex::Variant: {
            const size_t tuple_size = fuzz_rand() % 6 + 1;
            DataTypes elements;
            for (size_t i = 0; i < tuple_size; ++i)
                elements.push_back(getRandomType());
            return std::make_shared<DataTypeVariant>(elements);
        }
        case TypeIndex::Array: return std::make_shared<DataTypeArray>(getRandomType());
        case TypeIndex::Map: {
            auto key_type = getRandomType();
            /// `DataTypeMap`'s constructor rejects a `Nullable`/`LowCardinality(Nullable)` key.
            if (!DataTypeMap::isValidKeyType(key_type))
                key_type = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeMap>(key_type, getRandomType());
        }
        case TypeIndex::LowCardinality: {
            auto inner = getRandomType();
            if (!inner->canBeInsideLowCardinality())
                inner = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeLowCardinality>(inner);
        }
        case TypeIndex::Nullable: {
            auto inner = getRandomType();
            if (!inner->canBeInsideNullable())
                inner = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeNullable>(inner);
        }
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Decimal256)
        case TypeIndex::FixedString: return std::make_shared<DataTypeFixedString>(fuzz_rand() % 20 + 1);
        case TypeIndex::Enum8: {
            DataTypeEnum<Int8>::Values values;
            const size_t n = fuzz_rand() % 4 + 1;
            for (size_t i = 0; i < n; ++i)
                values.emplace_back("v" + std::to_string(i), static_cast<Int8>(i));
            return std::make_shared<DataTypeEnum<Int8>>(values);
        }
        case TypeIndex::Enum16: {
            DataTypeEnum<Int16>::Values values;
            const size_t n = fuzz_rand() % 4 + 1;
            for (size_t i = 0; i < n; ++i)
                values.emplace_back("v" + std::to_string(i), static_cast<Int16>(i));
            return std::make_shared<DataTypeEnum<Int16>>(values);
        }
        case TypeIndex::DateTime:
            return makeRandomDateTime();
        case TypeIndex::DateTime64:
            return makeRandomDateTime64(fuzz_rand() % 10);
        case TypeIndex::Time64:
            return std::make_shared<DataTypeTime64>(fuzz_rand() % 10);
        case TypeIndex::Dynamic:
            return std::make_shared<DataTypeDynamic>(fuzz_rand() % 20);
        case TypeIndex::Object:
            return makeRandomObject();
        case TypeIndex::QBit:
            return makeRandomQBit();
        case TypeIndex::AggregateFunction: {
            const size_t nargs = fuzz_rand() % 3;
            String name = "count";
            DataTypes arg_types;
            const auto & swap_aggrs = swapAggregateNames();
            if (nargs > 0 && swap_aggrs.contains(nargs))
            {
                name = pickRandomly(fuzz_rand, swap_aggrs.at(nargs));
                for (size_t i = 0; i < nargs; ++i)
                    arg_types.push_back(getRandomType());
            }
            if (auto random = makeAggregateFunctionType(name, arg_types, Array{}, /*simple=*/false))
                return random;
            /// Fall back to argument-less count, built directly: the helper can return null and getRandomType
            /// must never hand a null type to its callers.
            AggregateFunctionProperties properties;
            auto count = AggregateFunctionFactory::instance().get("count", NullsAction::EMPTY, {}, {}, properties);
            return std::make_shared<DataTypeAggregateFunction>(count, DataTypes{}, Array{});
        }
        default: break;
    }

#undef DISPATCH
    /// NOLINTEND(bugprone-macro-parentheses)

    return DataTypeFactory::instance().get(String(magic_enum::enum_name(type_id)));
}
}
