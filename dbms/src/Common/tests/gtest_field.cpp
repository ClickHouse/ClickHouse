#include <gtest/gtest.h>

#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>

using namespace DB;

TEST(Common, Field)
{
    auto test = [](Field field)
    {
        std::cout << "field: " << applyVisitor(FieldVisitorToString(), field) << "\n";

        SipHash hash1;
        applyVisitor(FieldVisitorHash(hash1), field);
        std::cout << "hash1: " << hash1.get64() << "\n";
        
        SipHash hash2;
        hash2.update(field.reinterpret<const char *>());
        std::cout << "hash2: " << hash2.get64() << "\n";
    };

    test(1);
    test("kek");
}
