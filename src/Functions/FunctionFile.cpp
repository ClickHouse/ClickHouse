#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Interpreters/Context.h>
#include <unistd.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int NOT_IMPLEMENTED;
        extern const int FILE_DOESNT_EXIST;
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_CLOSE_FILE;
        extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
        extern const int INCORRECT_FILE_NAME;
        extern const int DATABASE_ACCESS_DENIED;
    }

    /** A function to read file as a string.
  */
    class FunctionFile : public IFunction
    {
    public:
        static constexpr auto name = "file";
        static FunctionPtr create(const Context &context) { return std::make_shared<FunctionFile>(context); }
        explicit FunctionFile(const Context &context_) : context(context_) {}

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }
        bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (!isString(arguments[0].type))
                throw Exception(getName() + " is only implemented for types String", ErrorCodes::NOT_IMPLEMENTED);
            return std::make_shared<DataTypeString>();
        }

        bool useDefaultImplementationForConstants() const override { return true; }

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

                //File access permission check
                const String user_files_path = context.getUserFilesPath();
                String user_files_absolute_path = Poco::Path(user_files_path).makeAbsolute().makeDirectory().toString();
                Poco::Path poco_filepath = Poco::Path(filename);
                if (poco_filepath.isRelative())
                    poco_filepath = Poco::Path(user_files_absolute_path, poco_filepath);
                const String file_absolute_path = poco_filepath.absolute().toString();
                checkReadIsAllowed(user_files_absolute_path, file_absolute_path);

                //Method-1: Read file with ReadBuffer
                ReadBufferFromFile in(file_absolute_path);
                ssize_t file_len = Poco::File(file_absolute_path).getSize();
                res_chars.resize_exact(file_len + 1);
                char *res_buf = reinterpret_cast<char *>(&res_chars[0]);
                in.readStrict(res_buf, file_len);

                /*
                //Method-2(Just for reference): Read directly into the String buf, which avoiding one copy from PageCache to ReadBuffer
                int fd;
                if (-1 == (fd = open(file_absolute_path.c_str(), O_RDONLY)))
                     throwFromErrnoWithPath("Cannot open file " + std::string(file_absolute_path), std::string(file_absolute_path),
                                           errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
                if (file_len != pread(fd, res_buf, file_len, 0))
                    throwFromErrnoWithPath("Read failed with " + std::string(file_absolute_path), std::string(file_absolute_path),
                                           ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
                if (0 != close(fd))
                    throw Exception("Cannot close file " + std::string(file_absolute_path), ErrorCodes::CANNOT_CLOSE_FILE);
                fd = -1;
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
        void checkReadIsAllowed(const std::string & user_files_absolute_path, const std::string & file_absolute_path) const
        {
            // If run in Local mode, no need for path checking.
            if (context.getApplicationType() != Context::ApplicationType::LOCAL)
                if (file_absolute_path.find(user_files_absolute_path) != 0)
                    throw Exception("File is not inside " + user_files_absolute_path, ErrorCodes::DATABASE_ACCESS_DENIED);

            Poco::File path_poco_file = Poco::File(file_absolute_path);
            if (path_poco_file.exists() && path_poco_file.isDirectory())
                throw Exception("File can't be a directory", ErrorCodes::INCORRECT_FILE_NAME);
        }

        const Context & context;
    };


    void registerFunctionFile(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionFile>();
    }

}
