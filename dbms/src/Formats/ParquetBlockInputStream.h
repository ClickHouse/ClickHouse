#pragma once

#include <Common/config.h>
#if USE_PARQUET

#    include <DataStreams/IProfilingBlockInputStream.h>
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#    if defined(__clang__) && __clang_major__ >= 7
#        pragma GCC diagnostic ignored "-Wc++98-compat-extra-semi"
#    endif
#    include <arrow/api.h>
#    pragma GCC diagnostic pop

namespace DB
{

class Context;

class ParquetBlockInputStream : public IProfilingBlockInputStream
{
public:
    ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_);

    String getName() const override { return "Parquet"; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;

    static void fillColumnWithStringData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);
    static void fillColumnWithBooleanData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);
    static void fillColumnWithDate32Data(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);
    static void fillColumnWithDate64Data(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);
    template <typename NumericType>
    static void fillColumnWithNumericData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);

    static void fillByteMapFromArrowColumn(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & bytemap);

    static const std::unordered_map<arrow::Type::type, std::shared_ptr<IDataType>> arrow_type_to_internal_type;

    // TODO: check that this class implements every part of its parent

    const Context & context;
};

}

#endif
