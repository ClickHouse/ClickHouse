#include <DataStreams/CastEnumBlockInputStream.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ExpressionActions.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{

CastEnumBlockInputStream::CastEnumBlockInputStream(
    const Context & context_,
    BlockInputStreamPtr input_,
    const Block & in_sample_,
    const Block & out_sample_)
    : context(context_)
{
    collectEnums(in_sample_, out_sample_);
    cast_functions.resize(in_sample_.columns());
    children.push_back(input_);
}

String CastEnumBlockInputStream::getName() const
{
    return "CastEnum";
}

String CastEnumBlockInputStream::getID() const
{
    std::stringstream res;
    res << "CastEnum(" << children.back()->getID() << ")";
    return res.str();
}

Block CastEnumBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block || enum_types.empty())
        return block;

    Block res;
    size_t s = block.columns();

    for (size_t i = 0; i < s; ++i)
    {
        const auto & elem = block.getByPosition(i);

        if (bool(enum_types[i]))
        {
            const auto & type = static_cast<const IDataTypeEnum *>(enum_types[i]->type.get());
            Block temporary_block
            {
                {
                    elem.column,
                    elem.type,
                    elem.name
                },
                {
                    std::make_shared<ColumnConstString>(1, type->getName()),
                    std::make_shared<DataTypeString>(),
                    ""
                },
                {
                    nullptr,
                    enum_types[i]->type,
                    ""
                }
            };

            FunctionPtr & cast_function = cast_functions[i];

            /// Initialize function.
            if (!cast_function)
            {
                cast_function = FunctionFactory::instance().get("CAST", context);

                DataTypePtr unused_return_type;
                ColumnsWithTypeAndName arguments{ temporary_block.getByPosition(0), temporary_block.getByPosition(1) };
                std::vector<ExpressionAction> unused_prerequisites;

                /// Prepares function to execution. TODO It is not obvious.
                cast_function->getReturnTypeAndPrerequisites(arguments, unused_return_type, unused_prerequisites);
            }

            cast_function->execute(temporary_block, {0, 1}, 2);

            res.insert({
                temporary_block.getByPosition(2).column,
                enum_types[i]->type,
                enum_types[i]->name});
        }
        else
        {
            res.insert(elem);
        }
    }

    return res;
}

void CastEnumBlockInputStream::collectEnums(const Block & in_sample, const Block & out_sample)
{
    size_t in_size = in_sample.columns();
    enum_types.resize(in_size);
    for (size_t i = 0; i < in_size; ++i)
    {
        const auto & in_elem  = in_sample.getByPosition(i);
        const auto & out_elem = out_sample.getByPosition(i);

        /// Force conversion if source type is not Enum.
        if ( dynamic_cast<IDataTypeEnum*>(out_elem.type.get()) &&
            !dynamic_cast<IDataTypeEnum*>(in_elem.type.get()))
        {
            enum_types[i] = NameAndTypePair(out_elem.name, out_elem.type);
        }

        /// Force conversion if both types is numeric but not equal.
        if (in_elem.type->behavesAsNumber() && out_elem.type->behavesAsNumber() && !out_elem.type->equals(*in_elem.type))
        {
            enum_types[i] = NameAndTypePair(out_elem.name, out_elem.type);
        }
    }
}

}
