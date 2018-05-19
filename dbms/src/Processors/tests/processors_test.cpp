#include <iostream>
#include <Processors/Processor.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


using namespace DB;


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource()
        : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
    {
    }

private:
    UInt64 current_number = 0;

    Block generate() override
    {
        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create(1, current_number));
        ++current_number;
        return getPort().getHeader().cloneWithColumns(std::move(columns));
    }
};


class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    PrintSink()
        : ISink(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }}))
    {
    }

private:
    WriteBufferFromFileDescriptor out{STDOUT_FILENO};

    void consume(Block && block) override
    {
        size_t rows = block.rows();
        size_t columns = block.columns();

        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            for (size_t column_num = 0; column_num < columns; ++column_num)
            {
                if (column_num != 0)
                    writeChar('\t', out);
                getPort().getHeader().getByPosition(column_num).type->serializeText(*block.getByPosition(column_num).column, row_num, out);
            }
            writeChar('\n', out);
        }

        out.next();
    }
};


int main(int, char **)
try
{
    auto source = std::make_shared<NumbersSource>();
    auto sink = std::make_shared<PrintSink>();
    auto limit = std::make_shared<LimitTransform>(source->getPort().getHeader(), 100, 0);

    connect(source->getPort(), limit->getInputPort());
    connect(limit->getOutputPort(), sink->getPort());

    SequentialPipelineExecutor executor({source, limit, sink});

    while (true)
    {
        IProcessor::Status status = executor.getStatus();

        if (status == IProcessor::Status::Finished)
            break;
        else if (status == IProcessor::Status::Ready)
            executor.work();
        else
            throw Exception("Bad status");
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
