add_library(harmful harmful.c)
target_link_libraries(harmful global-group)
install(TARGETS harmful EXPORT global ARCHIVE DESTINATION lib)
