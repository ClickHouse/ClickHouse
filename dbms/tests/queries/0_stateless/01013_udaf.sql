drop table if exists udf_test;
create table udf_test (
    a UInt32,
    b String,
    c Enum8('one' = 1, 'two' = 2, 'three' = 3)
) engine=Memory;

insert into udf_test
values (1, 'a', 'one'), (2, 'bb', 'two'), (3, 'ccc', 'three');

select udaf('
    using Data = std::string;

    void udafAdd(
        Data & data,
        uint32_t a,
        std::string b,
        int8_t c
    )
    {
        if (!data.empty())
            data += ",";

        data += std::to_string(a) + b + std::to_string(c);
    }

    void udafMerge(
        Data & data,
        const Data & rhs
    )
    {
        if (!data.empty())
            data += ",";

        data += rhs;
    }

    std::string udafReduce(
        const Data & data
    )
    {
        return data + "!";
    }
')(a, b, c) as result
from udf_test;

select udaf('
    using Data = std::vector<uint32_t>;

    void udafAdd(
        Data & data,
        std::vector<uint32_t> x
    )
    {
        data.insert(data.end(), x.begin(), x.end());
    }

    void udafMerge(
        Data & data,
        const Data & rhs
    )
    {
        data.insert(data.end(), rhs.begin(), rhs.end());
    }

    Data udafReduce(
        const Data & data
    )
    {
        return data;
    }
')([a, 123]) as result
from udf_test;
