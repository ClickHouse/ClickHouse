#include <iostream>
#include <iomanip>
#include <sstream>

#include <Core/Field.h>
#include <Common/FieldVisitors.h>

#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypeString.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    FieldVisitorToString to_string;

    Field field = UInt64(0);
    std::cerr << applyVisitor(to_string, field) << std::endl;

    field = std::string("Hello, world!");
    std::cerr << applyVisitor(to_string, field) << std::endl;

    field = Null();
    std::cerr << applyVisitor(to_string, field) << std::endl;

    Field field2;
    field2 = field;
    std::cerr << applyVisitor(to_string, field2) << std::endl;

    Array array;
    array.push_back(UInt64(123));
    array.push_back(Int64(-123));
    array.push_back(String("Hello"));
    field = array;
    std::cerr << applyVisitor(to_string, field) << std::endl;

    get<Array &>(field).push_back(field);
    std::cerr << applyVisitor(to_string, field) << std::endl;

    std::cerr << (field < field2) << std::endl;
    std::cerr << (field2 < field) << std::endl;


    try
    {
        size_t n = argc == 2 ? parse<UInt64>(argv[1]) : 10000000;

        Stopwatch watch;

        {
            Array array(n);

            {
                Stopwatch watch;

                for (size_t i = 0; i < n; ++i)
                    array[i] = String(i % 32, '!');

                watch.stop();
                std::cerr << std::fixed << std::setprecision(2)
                    << "Set " << n << " fields (" << n * sizeof(array[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                    << n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(array[0]) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
                    << std::endl;
            }

            {
                Stopwatch watch;

                size_t sum = 0;
                for (size_t i = 0; i < n; ++i)
                    sum += safeGet<const String &>(array[i]).size();

                watch.stop();
                std::cerr << std::fixed << std::setprecision(2)
                    << "Got " << n << " fields (" << n * sizeof(array[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                    << n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(array[0]) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
                    << std::endl;

                std::cerr << sum << std::endl;
            }

            watch.restart();
        }

        watch.stop();

        std::cerr << std::fixed << std::setprecision(2)
            << "Destroyed " << n << " fields (" << n * sizeof(Array::value_type) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
            << n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(Array::value_type) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
            << std::endl;
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    std::cerr << "sizeof(Field) = " << sizeof(Field) << std::endl;

    return 0;
}
