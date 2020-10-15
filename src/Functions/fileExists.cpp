#include <Interpreters/Context.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>

#include <Poco/Path.h>
#include <Poco/File.h>

#include <Common/StringUtils/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int PATH_ACCESS_DENIED;
}

/// fileExists(x) - returns 1 if and only if a file or directory exists
class ExecutableFunctionFileExists : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionFileExists(const String & user_files_path_) : user_files_path{user_files_path_} {}

    static constexpr auto name = "fileExists";
    String getName() const override { return name; }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * arg_column = block[arguments[0]].column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        Poco::Path file_path = Poco::Path(arg_string->getDataAt(0).toString());
        if (file_path.isRelative())
            file_path = Poco::Path(user_files_path, file_path);

        if (!startsWith(file_path.toString(), user_files_path))
            throw Exception("File path " + file_path.toString() + " is not inside " + user_files_path, ErrorCodes::PATH_ACCESS_DENIED);

        block[result].column = DataTypeUInt8().createColumnConst(input_rows_count, Poco::File(file_path).exists());
    }

private:
    const String user_files_path;
};

class FunctionBaseFileExists : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseFileExists(const String & user_files_path_, DataTypePtr return_type_)
        : user_files_path(user_files_path_), return_type(return_type_)
    {
    }

    static constexpr auto name = "fileExists";
    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override
    {
        DataTypes argument_types;
        argument_types.emplace_back(std::make_shared<DataTypeString>());

        return argument_types;
    }

    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_unique<ExecutableFunctionFileExists>(user_files_path);
    }

private:
    const String user_files_path;
    DataTypePtr return_type;
};

class FunctionOverloadResolverFileExists : public IFunctionOverloadResolverImpl
{
public:
    explicit FunctionOverloadResolverFileExists(const String & user_files_path_): user_files_path(user_files_path_) {}

    static FunctionOverloadResolverImplPtr create(const Context & context)
    {
        return std::make_unique<FunctionOverloadResolverFileExists>(context.getUserFilesPath());
    }

    static constexpr auto name = "fileExists";
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!checkColumnConst<ColumnString>(arguments.at(0).column.get()))
            throw Exception("The argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        return std::make_unique<FunctionBaseFileExists>(user_files_path, return_type);
    }

private:
    const String user_files_path;
};

void registerFunctionFileExists(FunctionFactory & factory)
{
    factory.registerFunction<FunctionOverloadResolverFileExists>(FunctionFactory::CaseInsensitive);
}

}