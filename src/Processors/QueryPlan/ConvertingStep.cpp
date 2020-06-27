#include <Processors/QueryPlan/ConvertingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}

static void filterDistinctColumns(const Block & res_header, NameSet & distinct_columns)
{
    if (distinct_columns.empty())
        return;

    NameSet new_distinct_columns;
    for (const auto & column : res_header)
        if (distinct_columns.count(column.name))
            new_distinct_columns.insert(column.name);

    distinct_columns.swap(new_distinct_columns);
}

ConvertingStep::ConvertingStep(const DataStream & input_stream_, Block result_header_)
        : ITransformingStep(
        input_stream_,
        result_header_,
        getTraits())
        , result_header(std::move(result_header_))
{
    /// Some columns may be removed
    filterDistinctColumns(output_stream->header, output_stream->distinct_columns);
    filterDistinctColumns(output_stream->header, output_stream->local_distinct_columns);
}

void ConvertingStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ConvertingTransform>(header, result_header, ConvertingTransform::MatchColumnsMode::Name);
    });
}

void ConvertingStep::describeActions(FormatSettings & settings) const
{
    const auto & header = input_streams[0].header;
    auto conversion = ConvertingTransform(header, result_header, ConvertingTransform::MatchColumnsMode::Name)
            .getConversion();

    auto dump_description = [&](const ColumnWithTypeAndName & elem, bool is_const)
    {
        settings.out << elem.name << ' ' << elem.type->getName() << (is_const ? " Const" : "") << '\n';
    };

    String prefix(settings.offset, ' ');

    for (size_t i = 0; i < conversion.size(); ++i)
    {
        const auto & from = header.getByPosition(conversion[i]);
        const auto & to = result_header.getByPosition(i);

        bool from_const = from.column && isColumnConst(*from.column);
        bool to_const = to.column && isColumnConst(*to.column);

        settings.out << prefix;

        if (from.name == to.name && from.type->equals(*to.type) && from_const == to_const)
            dump_description(from, from_const);
        else
        {
            dump_description(to, to_const);
            settings.out << " <- ";
            dump_description(from, from_const);
        }

        settings.out << '\n';
    }
}

}
