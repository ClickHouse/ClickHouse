add_library(hilite_comparator
    HiliteComparator.h
    HiliteComparator.cpp
)

target_link_libraries(hilite_comparator PRIVATE dbms)
