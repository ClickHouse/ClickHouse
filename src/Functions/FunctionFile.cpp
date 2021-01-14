#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <common/find_symbols.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}


/** A function to read file as a string.
  */
class FunctionFile : public IFunction
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFile>(); }
    static FunctionPtr create() { return std::make_shared<FunctionFile>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & column = arguments[0].column;
        const char * filename = nullptr;

        if (const auto * column_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & filename_chars = column_string->getChars();
            filename = reinterpret_cast<const char *>(&filename_chars[0]);

            auto fd = open(filename, O_RDONLY);
            if (-1 == fd)
                throwFromErrnoWithPath("Cannot open file " + std::string(filename), std::string(filename),
                                       errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
            struct stat file_stat;
            if (-1 == fstat(fd, &file_stat))
                throwFromErrnoWithPath("Cannot stat file " + std::string(filename), std::string(filename),
                                       ErrorCodes::CANNOT_FSTAT);

            auto file_length = static_cast<uint64_t>(file_stat.st_size);
            auto res = ColumnString::create();
            auto & res_chars = res->getChars();
            auto & res_offsets = res->getOffsets();
            res_chars.resize_exact(file_length + 1);
            res_offsets.push_back(file_length + 1);
            char * res_buf = reinterpret_cast<char *>(&res_chars[0]);

            //To read directly into the String buf, avoiding one redundant copy
            ssize_t bytes_read = pread(fd, res_buf, file_length, 0);
            if (bytes_read == -1)
                throwFromErrnoWithPath("Read failed for " + std::string(filename), std::string(filename),
                                   errno == EBADF ? ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR : ErrorCodes::ILLEGAL_COLUMN);
            if (static_cast<uint64_t>(bytes_read) != file_length)
                throwFromErrnoWithPath("Cannot read all bytes from " + std::string(filename), std::string(filename), ErrorCodes::ILLEGAL_COLUMN);

            res_buf[file_length] = '\0';
            close(fd);
            return res;
        }
        else
        {
            throw Exception("Bad Function arguments for file() " + std::string(filename), ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


void registerFunctionFromFile(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFile>();
}

}