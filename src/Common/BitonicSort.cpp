#include <string.h>
#include <iostream>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <type_traits>

#include <ext/bit_cast.h>
#include <Core/Types.h>
#include <Core/Defines.h>

#include "BitonicSort.h"

#define CL_USE_DEPRECATED_OPENCL_1_2_APIS


template <class T>
cl_program BitonicSort<T>::makeProgram(const char *file_name) {
    FILE* file = fopen(file_name, "rb");
    if (file == nullptr) {
        std::cout << "[1][file] failed";
        exit(EXIT_FAILURE);
    }

    std::cout << "[1][file] done\n";

    fseek(file, 0, SEEK_END);
    size_t source_size = ftell(file);
    rewind(file);

    char* source_code = static_cast<char*>(malloc(source_size + 1));
    fread(source_code, sizeof(char), source_size, file);
    source_code[source_size] = '\0';
    fclose(file);

    cl_int error = 0;
    const char** constSource = const_cast<const char**>(&source_code);
    cl_context context = configuration.gpuContext;
    cl_program program = clCreateProgramWithSource(context, 1, constSource,
                                                   &source_size, &error);
    if (error) {
        if (error == CL_INVALID_VALUE) {
            std::cout << "CL_INVALID_VALUE\n";
        } else if (error == CL_OUT_OF_RESOURCES) {
            std::cout << "CL_OUT_OF_RESOURCES\n";
        } else if (error == CL_INVALID_CONTEXT) {
            std::cout << "CL_INVALID_CONTEXT\n";
        } else if (error == CL_OUT_OF_HOST_MEMORY) {
            std::cout << "CL_OUT_OF_HOST_MEMORY\n";
        }

        std::cout << "[2][file] failed";
        exit(EXIT_FAILURE);
    }

    std::cout << "[2][file] done\n";

    free(source_code);

    error = clBuildProgram(program, 1, &configuration.device, "", nullptr, nullptr);

    if (error == CL_BUILD_PROGRAM_FAILURE) {
        size_t logSize;
        error = clGetProgramBuildInfo(program, configuration.device, CL_PROGRAM_BUILD_LOG, 0, nullptr, &logSize);
        if (error) {
            std::cout << "[3][file] failed";
            exit(EXIT_FAILURE);
        }

        std::cout << "[3][file] done";

        std::vector<char> log(logSize);
        clGetProgramBuildInfo(program, configuration.device, CL_PROGRAM_BUILD_LOG, logSize, log.data(), nullptr);
        if (error) {
            std::cout << "[4][file] failed";
            exit(EXIT_FAILURE);
        }

        std::cout << "[4][file] done";
        exit(EXIT_FAILURE);
        std::cout << "[5][file] failed";
    }

    if (error == CL_INVALID_PROGRAM)
        std::cout << "CL_INVALID_PROGRAM\n";
    else if (error == CL_INVALID_VALUE)
        std::cout << "CL_INVALID_VALUE\n";
    else if (error == CL_INVALID_DEVICE)
        std::cout << "CL_INVALID_DEVICE\n";
    else if (error == CL_INVALID_BINARY)
        std::cout << "CL_INVALID_BINARY\n";
    else if (error == CL_INVALID_BUILD_OPTIONS)
        std::cout << "CL_INVALID_BUILD_OPTIONS\n";
    else if (error == CL_INVALID_OPERATION)
        std::cout << "CL_INVALID_OPERATION\n";
    else if (error == CL_COMPILER_NOT_AVAILABLE)
        std::cout << "CL_COMPILER_NOT_AVAILABLE\n";
    else if (error == CL_BUILD_PROGRAM_FAILURE)
        std::cout << "CL_BUILD_PROGRAM_FAILURE\n";
    else if (error == CL_INVALID_OPERATION)
        std::cout << "CL_INVALID_OPERATION\n";
    else if (error == CL_OUT_OF_HOST_MEMORY)
        std::cout << "CL_OUT_OF_HOST_MEMORY\n";

    if (error) {
        std::cout << "[6][file] failed";
        exit(EXIT_FAILURE);
    }
    std::cout << "[6][file] done";
    return program;
}


template <class T>
void BitonicSort<T>::configure() {
//    cl_uint numPlatforms;
    cl_int error = 0;

    error = clGetPlatformIDs(1, &configuration.platform, nullptr);
    if (error) {
        std::cout << "[1] failed";
        exit(EXIT_FAILURE);
    }

//    error = clGetPlatformIDs(0, nullptr, &numPlatforms);
//    if (error || numPlatforms <= platformId) {
//        std::cout << "[1] failed";
//        exit(EXIT_FAILURE);
//    }
//
//    std::cout << "[1] done\n";
//
//    std::vector<cl_platform_id> platforms(numPlatforms);
//    error = clGetPlatformIDs(numPlatforms, platforms.data(), nullptr);
//
//    if (error) {
//        std::cout << "[2] failed";
//        exit(EXIT_FAILURE);
//    }
//
//    std::cout << "[2] done\n";
//
//    cl_platform_id platform = platforms[platformId];

//        cl_uint numDevices;
//        error = clGetDeviceIDs(platform_, deviceType, 0, nullptr, &numDevices);
//        if (error || numDevices <= deviceId)
//            throw OCL_EXCEPTION(err);
//
//        std::vector<cl_device_id> devices(numDevices);
//        error = clGetDeviceIDs(platform_, deviceType, numDevices, devices.data(), nullptr);
//        if (error)
//            throw OCL_EXCEPTION(err);
//        device_ = devices[deviceId];

    error = clGetDeviceIDs(configuration.platform, CL_DEVICE_TYPE_GPU, 1, &configuration.device, nullptr);

    if (error) {
        std::cout << "[2.5] failed";
        exit(EXIT_FAILURE);
    }

    configuration.gpuContext = clCreateContext(nullptr, 1, &configuration.device, nullptr, nullptr, &error);

    if (error) {
        std::cout << "[3] failed";
        exit(EXIT_FAILURE);
    }

    std::cout << "[3] done\n";

#ifdef CL_USE_DEPRECATED_OPENCL_1_2_APIS
    configuration.commandQueue = clCreateCommandQueue(configuration.gpuContext, configuration.device, 0, &error);
#else
    configuration.commandQueue = clCreateCommandQueueWithProperties(configuration.gpuContext, configuration.device, nullptr, &error);
#endif
    if (error) {
        std::cout << "[4] failed";
        exit(EXIT_FAILURE);
    }

    std::cout << "[4] done\n";

    cl_program program_ = makeProgram("Common/BitonicSort.cl");
    configuration.setProgram(program_);

    cl_kernel kernel = clCreateKernel(configuration.program(), "bitonic_sort", &error);

    if (error) {
        std::cout << "[5] failed";
        exit(EXIT_FAILURE);
    }

    std::cout << "[5] done\n";

    configuration.setKernel(kernel);
}


template <class T>
void BitonicSort<T>::prepareBuffers() {

}


template <class T>
void BitonicSort<T>::sort(T* data, size_t size) {
    int result = 0;
    for (size_t index = 0; index < size; ++index)
        result += data[index];
}
