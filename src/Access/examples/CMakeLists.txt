if (TARGET ch_contrib::krb5)
    clickhouse_add_executable (kerberos_init kerberos_init.cpp)
    target_link_libraries (kerberos_init PRIVATE dbms clickhouse_functions ch_contrib::krb5)
endif()
