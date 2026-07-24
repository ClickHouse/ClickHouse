#pragma once

/// Helper: build a DataTypeEnum8 from an X-macro list.
/// Before calling, temporarily #define ENUM_TYPE to the target enum.
/// Example:
///   #define ENUM_TYPE TokenType
///   return MAKE_ENUM8_TYPE(APPLY_FOR_TOKENS);
///   #undef ENUM_TYPE
#define ENUM8_ENTRY_(NAME) {#NAME, static_cast<Int8>(ENUM_TYPE::NAME)},
#define MAKE_ENUM8_TYPE(APPLY_MACRO) \
    std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{APPLY_MACRO(ENUM8_ENTRY_)})

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsBool implicit_select;
}

/// Parser settings extracted from context, used by highlightQuery.
struct QueryTokenizationSettings
{
    size_t max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;
    size_t max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;
    bool implicit_select = false;
};

/// Common base class for functions that tokenize/highlight queries.
/// Impl must provide:
///   static constexpr auto name = "...";
///   static DataTypePtr makeEnumType();
///   static void processRow(std::string_view query,
///                          PaddedPODArray<UInt64> & data_begin,
///                          PaddedPODArray<UInt64> & data_end,
///                          PaddedPODArray<Int8> & data_type,
///                          size_t & total,
///                          const QueryTokenizationSettings & settings);
template <typename Impl>
class FunctionQueryTokenization : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionQueryTokenization>(context);
    }

    explicit FunctionQueryTokenization(ContextPtr context)
    {
        if (context)
        {
            const auto & settings = context->getSettingsRef();
            parser_settings.max_parser_depth = settings[Setting::max_parser_depth];
            parser_settings.max_parser_backtracks = settings[Setting::max_parser_backtracks];
            parser_settings.implicit_select = settings[Setting::implicit_select];
        }
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}};
        validateFunctionArguments(*this, arguments, args);

        DataTypes types{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>(), Impl::makeEnumType()};
        Strings names{"begin", "end", "type"};
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(types), std::move(names)));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString & col_query = assert_cast<const ColumnString &>(*arguments[0].column);

        auto col_begin = ColumnUInt64::create();
        auto col_end = ColumnUInt64::create();
        auto col_type = ColumnInt8::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & data_begin = col_begin->getData();
        auto & data_end = col_end->getData();
        auto & data_type = col_type->getData();
        auto & offsets = col_offsets->getData();
        offsets.resize(input_rows_count);

        size_t total = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = col_query.getDataAt(i);

            Impl::processRow(query, data_begin, data_end, data_type, total, parser_settings);

            offsets[i] = total;
        }

        MutableColumns tuple_columns;
        tuple_columns.emplace_back(std::move(col_begin));
        tuple_columns.emplace_back(std::move(col_end));
        tuple_columns.emplace_back(std::move(col_type));

        return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(col_offsets));
    }

private:
    QueryTokenizationSettings parser_settings;
};

}
