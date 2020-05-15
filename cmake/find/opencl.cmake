if(ENABLE_OPENCL)

# Intel OpenCl driver: sudo apt install intel-opencl-icd
# TODO It's possible to add it as submodules: https://github.com/intel/compute-runtime/releases

# OpenCL applications should link wiht ICD loader
# sudo apt install opencl-headers ocl-icd-libopencl1
# sudo ln -s /usr/lib/x86_64-linux-gnu/libOpenCL.so.1.0.0 /usr/lib/libOpenCL.so

find_package(OpenCL REQUIRED)
if(OpenCL_FOUND)
    set(USE_OPENCL 1)
endif()

endif()

message(STATUS "Using opencl=${USE_OPENCL}: ${OpenCL_INCLUDE_DIRS} : ${OpenCL_LIBRARIES}")
