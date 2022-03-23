#include <iostream>
#include <arrow/type.h>
#include <arrow/type_fwd.h>


void test()
{
    auto value_field = arrow::field("value", arrow::utf8(), false);
    auto map_field = arrow::field("evenInfo", arrow::map(arrow::utf8(), value_field), false);
    auto map_type = std::dynamic_pointer_cast<arrow::MapType>(map_field->type());
    std::cout << "field:" << map_field->ToString() << std::endl;
    std::cout << "key_type: " << map_type->key_type()->ToString() << std::endl;
    std::cout << "val_type: " << map_type->value_type()->ToString() << std::endl;
    std::cout << "item_type: " << map_type->item_type()->ToString() << std::endl;

    arrow::MemoryPool * pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::Status status = arrow::MakeBuilder(pool, map_field->type(), &array_builder);
    std::shared_ptr<arrow::Array> arrow_array;
    status = array_builder->Finish(&arrow_array);
    std::cout << status.ToString() << std::endl;
}

int main()
{
    test();
    return 0;
}