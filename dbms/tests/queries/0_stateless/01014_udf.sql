drop table if exists udf_test;
create table udf_test (
    a UInt32,
    b String,
    c Enum8('one' = 1, 'two' = 2, 'three' = 3)
) engine=Memory;

insert into udf_test
values (1, 'a', 'one'), (2, 'bb', 'two'), (3, 'ccc', 'three');

select udsf('
    std::string udsf(
        uint32_t a,
        std::string b,
        int8_t c
    )
    {
        return std::to_string(a) + b + std::to_string(c);
    }
', a, b, c) as result
from udf_test;

select udsf('
    std::vector<std::string> udsf(
        std::tuple<uint32_t, std::string> ab,
        int8_t c
    )
    {
        return {
            std::to_string(std::get<0>(ab)),
            std::get<1>(ab) + std::to_string(c),
        };
    }
', (a, b), c) as result
from udf_test;

select udf('
    std::vector<std::string> udf(
        size_t size,
        std::vector<uint32_t> a,
        std::vector<std::string> b,
        std::vector<int8_t> c
    )
    {
        std::vector<std::string> result;
        result.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            result.push_back(
                std::to_string(a[i]) + b[i] + std::to_string(c[i])
            );
        }

        return result;
    }
', a, b, c) as result
from udf_test;

select udf('
    std::vector<std::string> udf(
        size_t size,
        std::vector<std::string> b,
        std::tuple<std::string, std::string> x
    )
    {
        std::vector<std::string> result;
        result.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            result.push_back(
                std::get<0>(x) + b[i] + std::get<1>(x)
            );
        }

        return result;
    }
', b, ('<', '>')) as result
from udf_test;

select udf('
    std::tuple<std::vector<uint32_t>, uint32_t> udf(
        size_t size,
        std::vector<uint32_t> a,
        std::vector<std::vector<uint32_t>> x
    )
    {
        uint32_t result1 = 0;
        uint32_t result2 = 0;

        for (size_t i = 0; i < size; ++i)
        {
            result1 += x[i][a[i] - 1] * a[i];
            result2 += x[i][3 - a[i]] * a[i];
        }

        return {
            {result1, result1},
            result2
        };
    }
', a, [1, 10, 100]) as result
from udf_test;
