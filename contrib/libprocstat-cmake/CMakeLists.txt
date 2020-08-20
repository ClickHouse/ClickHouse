SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/libprocstat)
add_library(libprocstat OBJECT 
    ${LIBRARY_DIR}/libprocstat.c 
    ${LIBRARY_DIR}/cd9660.c
    ${LIBRARY_DIR}/common_kvm.c
    ${LIBRARY_DIR}/core.c
    ${LIBRARY_DIR}/msdosfs.c
    ${LIBRARY_DIR}/smbfs.c
    ${LIBRARY_DIR}/udf.c)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_KVM_VNODE")
