#pragma once

#include <string.h>
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

template <typename T>
class BitonicSort
{
public:
    static BitonicSort& getInstance()
    {
        static BitonicSort instance = BitonicSort(); // Guaranteed to be destroyed.
        // Instantiated on first use.
        return instance;
    }

    void prepareBuffers();

    void sort(T* data, size_t size);


    /*
     * A platform is a specific OpenCL implementation, for instance AMD APP,
     * NVIDIA or Intel OpenCL. A context is a platform with a set of available
     * devices for that platform. And the devices are the actual processors (CPU, GPU etc.)
     * that perform calculations.
     */
    void configure();

private:

    using ProgramType = std::remove_reference<decltype(*cl_program())>::type;           // _cl_program
    using KernelType = std::remove_reference<decltype(*cl_kernel())>::type;             // _cl_kernel

    class Configuration
    {
    public:
        cl_program program() { return program_.get(); }
        cl_kernel kernel() { return kernel_.get(); }


        void setProgram(cl_program program) { program_ = std::shared_ptr<ProgramType>(program, clReleaseProgram); }
        void setKernel(cl_kernel kern) { kernel_ = std::shared_ptr<KernelType>(kern, clReleaseKernel); }

        cl_context gpuContext;
        cl_command_queue commandQueue;
        cl_platform_id platform = nullptr;
        cl_device_id device = nullptr;

    private:

        std::shared_ptr<ProgramType> program_;
        std::shared_ptr<KernelType> kernel_;
    };

    Configuration configuration = Configuration();

    cl_program makeProgram(const char *file_name);

    // C++ 03
    // ========
    // Don't forget to declare these two. You want to make sure they
    // are unacceptable otherwise you may accidentally get copies of
    // your singleton appearing.

    BitonicSort() {}
    BitonicSort(BitonicSort const &);              // Don't Implement
    void operator=(BitonicSort const &); // Don't implement
};
