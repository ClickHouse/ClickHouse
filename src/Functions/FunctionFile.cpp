#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int NOT_IMPLEMENTED;
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
                auto res = ColumnString::create();
                auto & res_chars = res->getChars();
                auto & res_offsets = res->getOffsets();

                //TBD: Here, need to restrict the access permission for only user_path...

                ReadBufferFromFile in(filename);

                // Method-1: Read the whole file at once
                size_t file_len = Poco::File(filename).getSize();
                res_chars.resize(file_len + 1);
                char *res_buf = reinterpret_cast<char *>(&res_chars[0]);
                in.readStrict(res_buf, file_len);

                /*
                //Method-2: Read with loop

                char *res_buf;
                size_t file_len = 0, rlen = 0, bsize = 4096;
                while (0 == file_len || rlen == bsize)
                {
                    file_len += rlen;
                    res_chars.resize(1 + bsize + file_len);
                    res_buf = reinterpret_cast<char *>(&res_chars[0]);
                    rlen = in.read(res_buf + file_len, bsize);
                }
                file_len += rlen;
                */


                res_offsets.push_back(file_len + 1);
                res_buf[file_len] = '\0';

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
