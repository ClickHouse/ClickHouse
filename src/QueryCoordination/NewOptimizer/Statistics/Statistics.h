#pragma once

namespace DB
{

class Statistics
{
public:
    void setOutputRowSize(size_t row_size)
    {
        output_row_size = row_size;
    }

    size_t getOutputRowSize() const
    {
        return output_row_size;
    }

private:
    size_t output_row_size;
};

}
