#include "IFunctionOld.h"

#include <DataTypes/Native.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_EMBEDDED_COMPILER
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"
#    include <llvm/IR/IRBuilder.h>
#    pragma GCC diagnostic pop
#endif

namespace DB
{

#if USE_EMBEDDED_COMPILER

static std::optional<DataTypes> removeNullables(const DataTypes & types)
{
    for (const auto & type : types)
    {
        if (!typeid_cast<const DataTypeNullable *>(type.get()))
            continue;
        DataTypes filtered;
        for (const auto & sub_type : types)
            filtered.emplace_back(removeNullable(sub_type));
        return filtered;
    }
    return {};
}

bool IFunction::isCompilable(const DataTypes & arguments) const
{
    if (useDefaultImplementationForNulls())
        if (auto denulled = removeNullables(arguments))
            return isCompilableImpl(*denulled);
    return isCompilableImpl(arguments);
}

llvm::Value * IFunction::compile(llvm::IRBuilderBase & builder, const DataTypes & arguments, Values values) const
{
    auto denulled_arguments = removeNullables(arguments);
    if (useDefaultImplementationForNulls() && denulled_arguments)
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);

        std::vector<llvm::Value*> unwrapped_values;
        std::vector<llvm::Value*> is_null_values;

        unwrapped_values.reserve(arguments.size());
        is_null_values.reserve(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto * value = values[i];

            WhichDataType data_type(arguments[i]);
            if (data_type.isNullable())
            {
                unwrapped_values.emplace_back(b.CreateExtractValue(value, {0}));
                is_null_values.emplace_back(b.CreateExtractValue(value, {1}));
            }
            else
            {
                unwrapped_values.emplace_back(value);
            }
        }

        auto * result = compileImpl(builder, *denulled_arguments, unwrapped_values);

        auto * nullable_structure_type = toNativeType(b, makeNullable(getReturnTypeImpl(*denulled_arguments)));
        auto * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);

        auto * nullable_structure_with_result_value = b.CreateInsertValue(nullable_structure_value, result, {0});
        auto * nullable_structure_result_null = b.CreateExtractValue(nullable_structure_with_result_value, {1});

        for (auto * is_null_value : is_null_values)
            nullable_structure_result_null = b.CreateOr(nullable_structure_result_null, is_null_value);

        return b.CreateInsertValue(nullable_structure_with_result_value, nullable_structure_result_null, {1});
    }

    return compileImpl(builder, arguments, std::move(values));
}

#endif

}
