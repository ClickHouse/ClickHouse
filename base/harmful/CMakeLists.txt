add_library(harmful harmful.c)
target_link_libraries(global-libs INTERFACE harmful)
install(TARGETS harmful EXPORT global ARCHIVE DESTINATION lib)
