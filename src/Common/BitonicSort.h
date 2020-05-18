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
#include <map>
#include <type_traits>

#include <ext/bit_cast.h>
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Common/PODArray.h>
#include <Columns/ColumnsCommon.h>

#include "oclBasics.h"
#include "bitonicSortKernels.cl"

class BitonicSort
{
public:

    static BitonicSort & getInstance()
    {
        static BitonicSort instance = BitonicSort();
        return instance;
    }

    /// Sorts given array in specified order. Returns `true` if given sequence was sorted, `false` otherwise.
    template <typename T>
    bool sort(const DB::PaddedPODArray<T> & data, DB::IColumn::Permutation & res, cl_uint sort_ascending)
    {
        size_t s = data.size();

        /// Getting the nearest power of 2.
        size_t power = 1;

        if (s <= 8) power = 8;
        else while (power < s) power <<= 1;

        /// Allocates more space for additional stubs to be added if needed.
        std::vector<T> pairs_content(power);
        std::vector<UInt32> pairs_indices(power);
        for (UInt32 i = 0; i < s; ++i)
        {
            pairs_content[i] = data[i];
            pairs_indices[i] = i;
        }

        bool result = sort(pairs_content.data(), pairs_indices.data(), s, power - s, sort_ascending);

        if (!result) return false;

        for (size_t i = 0, shift = 0; i < power; ++i)
        {
            if (pairs_indices[i] >= s)
            {
                ++shift;
                continue;
            }
            res[i - shift] = pairs_indices[i];
        }

        return true;
    }

    /// Creating a configuration instance with making all OpenCl required variables
    /// such as device, platform, context, queue, program and kernel.
    void configure()
    {
        OCL::Settings settings = OCL::Settings(1, nullptr, 1, nullptr, 1, 0);

        cl_platform_id platform = OCL::getPlatformID(settings);
        cl_device_id device = OCL::getDeviceID(platform, settings);
        cl_context gpu_context = OCL::makeContext(device, settings);
        cl_command_queue command_queue = OCL::makeCommandQueue(device, gpu_context, settings);

        cl_program program = OCL::makeProgram(bitonic_sort_kernels, gpu_context, device, settings);

        /// Creating kernels for each specified data type.
        cl_int error = 0;

        kernels["char"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_char", &error),
                                                      clReleaseKernel);
        kernels["uchar"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_uchar", &error),
                                                       clReleaseKernel);
        kernels["short"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_short", &error),
                                                       clReleaseKernel);
        kernels["ushort"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_ushort", &error),
                                                        clReleaseKernel);
        kernels["int"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_int", &error),
                                                     clReleaseKernel);
        kernels["uint"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_uint", &error),
                                                      clReleaseKernel);
        kernels["long"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_long", &error),
                                                      clReleaseKernel);
        kernels["ulong"] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_ulong", &error),
                                                       clReleaseKernel);
        OCL::checkError(error);

        configuration = std::shared_ptr<OCL::Configuration>(new OCL::Configuration(device, gpu_context, command_queue, program));
    }

private:
    /// Dictionary with kernels for each type from list: uchar, char, ushort, short, uint, int, ulong and long.
    std::map<std::string, std::shared_ptr<KernelType>> kernels;
    /// Current configuration with core OpenCL instances.
    std::shared_ptr<OCL::Configuration> configuration = nullptr;

    /// Returns `true` if given sequence was sorted, `false` otherwise.
    template <typename T>
    bool sort(T * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        if (typeid(T).name() == typeid(cl_char).name())
            sort_char(reinterpret_cast<cl_char *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_uchar))
            sort_uchar(reinterpret_cast<cl_uchar *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_short))
            sort_short(reinterpret_cast<cl_short *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_ushort))
            sort_ushort(reinterpret_cast<cl_ushort *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_int))
            sort_int(reinterpret_cast<cl_int *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_uint))
            sort_uint(reinterpret_cast<cl_uint *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_long))
            sort_long(reinterpret_cast<cl_long *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else if (typeid(T) == typeid(cl_ulong))
            sort_ulong(reinterpret_cast<cl_ulong *>(p_input), indices, array_size, number_of_stubs, sort_ascending);
        else
            return false;

        return true;
    }

    /// Specific functions for each integer type.
    void sort_char(cl_char * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_char stubs_value = sort_ascending ? CHAR_MAX : CHAR_MIN;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["char"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_uchar(cl_uchar * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_uchar stubs_value = sort_ascending ? UCHAR_MAX : 0;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["uchar"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_short(cl_short * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_short stubs_value = sort_ascending ? SHRT_MAX : SHRT_MIN;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["short"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_ushort(cl_ushort * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_ushort stubs_value = sort_ascending ? USHRT_MAX : 0;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["ushort"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_int(cl_int * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_int stubs_value = sort_ascending ? INT_MAX : INT_MIN;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["int"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_uint(cl_uint * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_uint stubs_value = sort_ascending ? UINT_MAX : 0;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["uint"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_long(cl_long * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_long stubs_value = sort_ascending ? LONG_MAX : LONG_MIN;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["long"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    void sort_ulong(cl_ulong * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending)
    {
        cl_ulong stubs_value = sort_ascending ? ULONG_MAX : 0;
        fillWithStubs(number_of_stubs, stubs_value, p_input, indices, array_size);
        sort(kernels["ulong"].get(), p_input, indices, array_size + number_of_stubs, sort_ascending);
    }

    /// Sorts p_input inplace with indices. Works only with arrays which size equals to power of two.
    template <class T>
    void sort(cl_kernel kernel, T * p_input, cl_uint * indices, cl_int array_size, cl_uint sort_ascending)
    {
        cl_int error = CL_SUCCESS;
        cl_int num_stages = 0;

        for (cl_int temp = array_size; temp > 2; temp >>= 1)
            num_stages++;

        /// Creating OpenCL buffers using input arrays memory.
        cl_mem cl_input_buffer = OCL::createBuffer<T>(p_input, array_size, configuration.get()->context());
        cl_mem cl_indices_buffer = OCL::createBuffer<cl_uint>(indices, array_size, configuration.get()->context());

        configureKernel<cl_mem>(kernel, 0, static_cast<void *>(&cl_input_buffer));
        configureKernel<cl_mem>(kernel, 1, static_cast<void *>(&cl_indices_buffer));
        configureKernel<cl_uint>(kernel, 4, static_cast<void *>(&sort_ascending));

        for (cl_int stage = 0; stage < num_stages; stage++)
        {
            configureKernel<cl_uint>(kernel, 2, static_cast<void *>(&stage));

            for (cl_int pass_of_stage = stage; pass_of_stage >= 0; pass_of_stage--)
            {
                configureKernel<cl_uint>(kernel, 3, static_cast<void *>(&pass_of_stage));

                /// Setting work-item dimensions.
                size_t gsize = array_size / (2 * 4);
                size_t global_work_size[1] = {pass_of_stage ? gsize : gsize << 1 }; // number of quad items in input array

                /// Executing kernel.
                error = clEnqueueNDRangeKernel(configuration.get()->commandQueue(), kernel, 1, nullptr,
                                               global_work_size, nullptr, 0, nullptr, nullptr);
                OCL::checkError(error);
            }
        }

        /// Syncs all threads.
        OCL::finishCommandQueue(configuration.get()->commandQueue());

        OCL::releaseData(p_input, array_size, cl_input_buffer, configuration.get()->commandQueue());
        OCL::releaseData(indices, array_size, cl_indices_buffer, configuration.get()->commandQueue());
    }

    template <class T>
    void configureKernel(cl_kernel kernel, int number_of_argument, void * source)
    {
        cl_int error = clSetKernelArg(kernel, number_of_argument, sizeof(T), source);
        OCL::checkError(error);
    }

    /// Fills given sequences from `arraySize` index with `numberOfStubs` values.
    template <class T>
    void fillWithStubs(cl_int number_of_stubs, T value, T * p_input,
                       cl_uint * indices, cl_int array_size)
    {
        for (cl_int index = 0; index < number_of_stubs; ++index)
        {
            p_input[array_size + index] = value;
            indices[array_size + index] = array_size + index;
        }
    }

    BitonicSort() {}
    BitonicSort(BitonicSort const &);
    void operator=(BitonicSort const &);
};
