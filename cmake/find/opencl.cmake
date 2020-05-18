# TODO: enable by default
if(0)
    option(ENABLE_OPENCL "Enable OpenCL support" ${ENABLE_LIBRARIES})
endif()

if(ENABLE_OPENCL)

# Intel OpenCl driver: sudo apt install intel-opencl-icd
# @sa https://github.com/intel/compute-runtime/releases

# OpenCL applications should link wiht ICD loader
# sudo apt install opencl-headers ocl-icd-libopencl1
# sudo ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so
# TODO: add https://github.com/OCL-dev/ocl-icd as submodule instead

find_package(OpenCL)
if(OpenCL_FOUND)
    set(USE_OPENCL 1)
endif()

endif()

message(STATUS "Using opencl=${USE_OPENCL}: ${OpenCL_INCLUDE_DIRS} : ${OpenCL_LIBRARIES}")
