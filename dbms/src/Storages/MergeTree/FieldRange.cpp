#include <Storages/MergeTree/FieldRange.h>
#include <Common/FieldVisitors.h>
#include <sstream>


namespace DB {
    String Range::toString() const {
        std::stringstream str;

        if (!left_bounded)
            str << "(-inf, ";
        else
            str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left)
                << ", ";

        if (!right_bounded)
            str << "+inf)";
        else
            str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')')


        return str.str();
    }

}