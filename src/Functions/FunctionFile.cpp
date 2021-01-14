//#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnFixedString.h>
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
}


/** Conversion to fixed string is implemented only for strings.
  */
class FunctionFromFile : public IFunction
{
public:
    static constexpr auto name = "file";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFromFile>(); }
    static FunctionPtr create() { return std::make_shared<FunctionFromFile>(); }
    //static FunctionPtr create(const Context & context) { return std::make_shared<ConcatImpl>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    //bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);
        //??how to get accurate length  here? or should we return normal string type?
        //return std::make_shared<DataTypeFixedString>(1);
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    //ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & column = arguments[0].column;
        const char * filename = nullptr;
        // if (const auto * column_string = checkAndGetColumnConst<ColumnString>(column.get()))
        if (const auto * column_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & filename_chars = column_string->getChars();
            filename = reinterpret_cast<const char *>(&filename_chars[0]);

 /*
            //get file path
            auto user_files_path = Context::getUserFilesPath();


            String user_files_absolute_path = Poco::Path(user_files_path).makeAbsolute().makeDirectory().toString();
            Poco::Path poco_path = Poco::Path(table_path);
            if (poco_path.isRelative())
                poco_path = Poco::Path(user_files_absolute_path, poco_path);
            else //need to judge if the absolute path is in userfilespath?
            const String path = poco_path.absolute().toString();

*/
            auto fd = open(filename, O_RDONLY);
            if (fd == -1)
            {//arguments[0].column->getName()
                throw Exception("Can't open " + std::string(filename), ErrorCodes::ILLEGAL_COLUMN); //ErrorCode need to be rectify
            }
            struct stat file_stat;
            if (fstat(fd, &file_stat) == -1)
            {
                throw Exception("Can't stat " + std::string(filename), ErrorCodes::ILLEGAL_COLUMN);
            }
            auto file_length = static_cast<uint64_t>(file_stat.st_size);
            auto res = ColumnString::create();
            auto & res_chars = res->getChars();
            auto & res_offsets = res->getOffsets();
            //res_chars.resize_fill(file_length + 1);
            //omit the copy op to only once.
            res_chars.resize_exact(file_length + 1);
            res_offsets.push_back(file_length + 1);
            char * buf = reinterpret_cast<char *>(&res_chars[0]);
            ssize_t bytes_read = pread(fd, buf, file_length, 0);

            if (bytes_read == -1)
            {
                throw Exception("Bad read of " + std::string(filename), ErrorCodes::ILLEGAL_COLUMN);
            }
            if (static_cast<uint64_t>(bytes_read) != file_length)
            {
                throw Exception("Short read of " + std::string(filename), ErrorCodes::ILLEGAL_COLUMN);
            }
            buf[file_length] = '\0';
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
    factory.registerFunction<FunctionFromFile>();
}

}