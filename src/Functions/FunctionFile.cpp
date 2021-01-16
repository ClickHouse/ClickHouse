#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFile.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int NOT_IMPLEMENTED;
    }

    void checkCreationIsAllowed(const Context & context_global, const std::string & db_dir_path, const std::string & table_path);


    inline bool startsWith2(const std::string & s, const std::string & prefix)
    {
        return s.size() >= prefix.size() && 0 == memcmp(s.data(), prefix.data(), prefix.size());
    }

    void checkCreationIsAllowed(const Context & context_global, const std::string & db_dir_path, const std::string & table_path)
    {
        if (context_global.getApplicationType() != Context::ApplicationType::SERVER)
            return;

        /// "/dev/null" is allowed for perf testing
        if (!startsWith2(table_path, db_dir_path) && table_path != "/dev/null")
            throw Exception("File is not inside " + db_dir_path, 9);

        Poco::File table_path_poco_file = Poco::File(table_path);
        if (table_path_poco_file.exists() && table_path_poco_file.isDirectory())
            throw Exception("File must not be a directory", 9);
    }

    /** A function to read file as a string.
  */
    class FunctionFile : public IFunction
    {
    public:
        static constexpr auto name = "file";
        static FunctionPtr create(const Context &context) { return std::make_shared<FunctionFile>(context); }
        //static FunctionPtr create() { return std::make_shared<FunctionFile>(); }
        explicit FunctionFile(const Context &context_) : context(context_) {};
        //FunctionFile() {};

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

                //File_path access permission check.
                const String user_files_path = context.getUserFilesPath();
                String user_files_absolute_path = Poco::Path(user_files_path).makeAbsolute().makeDirectory().toString();
                Poco::Path poco_filepath = Poco::Path(filename);
                if (poco_filepath.isRelative())
                    poco_filepath = Poco::Path(user_files_absolute_path, poco_filepath);
                const String file_absolute_path = poco_filepath.absolute().toString();
                checkCreationIsAllowed(context, user_files_absolute_path, file_absolute_path);

                //Start read from file.
                ReadBufferFromFile in(filename);

                // Method-1: Read the whole file at once
                size_t file_len = Poco::File(filename).getSize();
                res_chars.resize_exact(file_len + 1);
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

    private:
        const Context & context;
    };

    void registerFunctionFromFile(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionFile>();
    }

}
