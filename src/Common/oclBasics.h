#pragma once

#include <Common/config.h>
#if USE_OPENCL

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <algorithm>
#include <Core/Types.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OPENCL_ERROR;
}
}

struct OCL
{
    using KernelType = std::remove_reference<decltype(*cl_kernel())>::type;

    /**
     * Structure which represents the most essential settings of common OpenCl entities.
     */
    struct Settings
    {
        // Platform info
        cl_uint number_of_platform_entries;
        cl_uint * number_of_available_platforms;

        // Devices info
        cl_uint number_of_devices_entries;
        cl_uint * number_of_available_devices;

        // Context settings
        cl_context_properties * context_properties;

        void (* context_callback)(const char *, const void *, size_t, void *);

        void * context_callback_data;

        // Command queue settings
        cl_command_queue_properties command_queue_properties;

        // Build settings
        cl_uint number_of_program_source_pointers;

        void (* build_notification_routine)(cl_program, void *user_data);

        void * build_callback_data;
        char * build_options;

        Settings(cl_uint number_of_platform_entries_,
                 cl_uint * number_of_available_platforms_,
                 cl_uint number_of_devices_entries_,
                 cl_uint * number_of_available_devices_,
                 cl_uint number_of_program_source_pointers_,
                 cl_command_queue_properties command_queue_properties_,
                 cl_context_properties * context_properties_ = nullptr,
                 void * context_data_callback_ = nullptr,
                 void (* context_callback_)(const char *, const void *, size_t, void *) = nullptr,
                 void (* build_notification_routine_)(cl_program, void * user_data) = nullptr,
                 void * build_callback_data_ = nullptr,
                 char * build_options_ = nullptr)
         {
            this->number_of_platform_entries = number_of_platform_entries_;
            this->number_of_available_platforms = number_of_available_platforms_;
            this->number_of_devices_entries = number_of_devices_entries_;
            this->number_of_available_devices = number_of_available_devices_;
            this->number_of_program_source_pointers = number_of_program_source_pointers_;
            this->command_queue_properties = command_queue_properties_;
            this->context_properties = context_properties_;
            this->context_callback = context_callback_;
            this->context_callback_data = context_data_callback_;
            this->build_notification_routine = build_notification_routine_;
            this->build_callback_data = build_callback_data_;
            this->build_options = build_options_;
        }
    };


    /**
     * Configuration with already created OpenCl common entities.
     */
    class Configuration
    {
    public:

        Configuration(cl_device_id device, cl_context gpu_context,
                      cl_command_queue command_queue, cl_program program)
        {
            this->device_ = device;
            this->gpu_context_ = std::shared_ptr<ContextType>(gpu_context, clReleaseContext);
            this->command_queue_ = std::shared_ptr<CommandQueueType>(command_queue, clReleaseCommandQueue);
            this->program_ = std::shared_ptr<ProgramType>(program, clReleaseProgram);
        }

        cl_device_id device() { return device_; }

        cl_context context() { return gpu_context_.get(); }

        cl_command_queue commandQueue() { return command_queue_.get(); }

        cl_program program() { return program_.get(); }

    private:

        using ProgramType = std::remove_reference<decltype(*cl_program())>::type;
        using CommandQueueType = std::remove_reference<decltype(*cl_command_queue())>::type;
        using ContextType = std::remove_reference<decltype(*cl_context())>::type;

        cl_device_id device_;

        std::shared_ptr<ContextType> gpu_context_;
        std::shared_ptr<CommandQueueType> command_queue_;
        std::shared_ptr<ProgramType> program_;
    };


    static String opencl_error_to_str(cl_int error)
    {
#define CASE_CL_CONSTANT(NAME) case NAME: return #NAME;

        // Suppose that no combinations are possible.
        switch (error)
        {
            CASE_CL_CONSTANT(CL_SUCCESS)
            CASE_CL_CONSTANT(CL_DEVICE_NOT_FOUND)
            CASE_CL_CONSTANT(CL_DEVICE_NOT_AVAILABLE)
            CASE_CL_CONSTANT(CL_COMPILER_NOT_AVAILABLE)
            CASE_CL_CONSTANT(CL_MEM_OBJECT_ALLOCATION_FAILURE)
            CASE_CL_CONSTANT(CL_OUT_OF_RESOURCES)
            CASE_CL_CONSTANT(CL_OUT_OF_HOST_MEMORY)
            CASE_CL_CONSTANT(CL_PROFILING_INFO_NOT_AVAILABLE)
            CASE_CL_CONSTANT(CL_MEM_COPY_OVERLAP)
            CASE_CL_CONSTANT(CL_IMAGE_FORMAT_MISMATCH)
            CASE_CL_CONSTANT(CL_IMAGE_FORMAT_NOT_SUPPORTED)
            CASE_CL_CONSTANT(CL_BUILD_PROGRAM_FAILURE)
            CASE_CL_CONSTANT(CL_MAP_FAILURE)
            CASE_CL_CONSTANT(CL_MISALIGNED_SUB_BUFFER_OFFSET)
            CASE_CL_CONSTANT(CL_EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST)
            CASE_CL_CONSTANT(CL_COMPILE_PROGRAM_FAILURE)
            CASE_CL_CONSTANT(CL_LINKER_NOT_AVAILABLE)
            CASE_CL_CONSTANT(CL_LINK_PROGRAM_FAILURE)
            CASE_CL_CONSTANT(CL_DEVICE_PARTITION_FAILED)
            CASE_CL_CONSTANT(CL_KERNEL_ARG_INFO_NOT_AVAILABLE)
            CASE_CL_CONSTANT(CL_INVALID_VALUE)
            CASE_CL_CONSTANT(CL_INVALID_DEVICE_TYPE)
            CASE_CL_CONSTANT(CL_INVALID_PLATFORM)
            CASE_CL_CONSTANT(CL_INVALID_DEVICE)
            CASE_CL_CONSTANT(CL_INVALID_CONTEXT)
            CASE_CL_CONSTANT(CL_INVALID_QUEUE_PROPERTIES)
            CASE_CL_CONSTANT(CL_INVALID_COMMAND_QUEUE)
            CASE_CL_CONSTANT(CL_INVALID_HOST_PTR)
            CASE_CL_CONSTANT(CL_INVALID_MEM_OBJECT)
            CASE_CL_CONSTANT(CL_INVALID_IMAGE_FORMAT_DESCRIPTOR)
            CASE_CL_CONSTANT(CL_INVALID_IMAGE_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_SAMPLER)
            CASE_CL_CONSTANT(CL_INVALID_BINARY)
            CASE_CL_CONSTANT(CL_INVALID_BUILD_OPTIONS)
            CASE_CL_CONSTANT(CL_INVALID_PROGRAM)
            CASE_CL_CONSTANT(CL_INVALID_PROGRAM_EXECUTABLE)
            CASE_CL_CONSTANT(CL_INVALID_KERNEL_NAME)
            CASE_CL_CONSTANT(CL_INVALID_KERNEL_DEFINITION)
            CASE_CL_CONSTANT(CL_INVALID_KERNEL)
            CASE_CL_CONSTANT(CL_INVALID_ARG_INDEX)
            CASE_CL_CONSTANT(CL_INVALID_ARG_VALUE)
            CASE_CL_CONSTANT(CL_INVALID_ARG_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_KERNEL_ARGS)
            CASE_CL_CONSTANT(CL_INVALID_WORK_DIMENSION)
            CASE_CL_CONSTANT(CL_INVALID_WORK_GROUP_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_WORK_ITEM_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_GLOBAL_OFFSET)
            CASE_CL_CONSTANT(CL_INVALID_EVENT_WAIT_LIST)
            CASE_CL_CONSTANT(CL_INVALID_EVENT)
            CASE_CL_CONSTANT(CL_INVALID_OPERATION)
            CASE_CL_CONSTANT(CL_INVALID_GL_OBJECT)
            CASE_CL_CONSTANT(CL_INVALID_BUFFER_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_MIP_LEVEL)
            CASE_CL_CONSTANT(CL_INVALID_GLOBAL_WORK_SIZE)
            CASE_CL_CONSTANT(CL_INVALID_PROPERTY)
            CASE_CL_CONSTANT(CL_INVALID_IMAGE_DESCRIPTOR)
            CASE_CL_CONSTANT(CL_INVALID_COMPILER_OPTIONS)
            CASE_CL_CONSTANT(CL_INVALID_LINKER_OPTIONS)
            CASE_CL_CONSTANT(CL_INVALID_DEVICE_PARTITION_COUNT)
            default:
                return "UNKNOWN ERROR CODE ";
        }

#undef CASE_CL_CONSTANT
    }


    static void checkError(cl_int error)
    {
        if (error != CL_SUCCESS)
            throw DB::Exception("OpenCL error: " + opencl_error_to_str(error), DB::ErrorCodes::OPENCL_ERROR);
    }


    /// Getting OpenCl main entities.

    static cl_platform_id getPlatformID(const Settings & settings)
    {
        cl_platform_id platform;
        cl_int error = clGetPlatformIDs(settings.number_of_platform_entries, &platform,
                                        settings.number_of_available_platforms);
        checkError(error);
        return platform;
    }

    static cl_device_id getDeviceID(cl_platform_id & platform, const Settings & settings)
    {
        cl_device_id device;
        cl_int error = clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, settings.number_of_devices_entries,
                                      &device, settings.number_of_available_devices);
        OCL::checkError(error);
        return device;
    }

    static cl_context makeContext(cl_device_id & device, const Settings & settings)
    {
        cl_int error;
        cl_context gpu_context = clCreateContext(settings.context_properties, settings.number_of_devices_entries,
                                                 &device, settings.context_callback, settings.context_callback_data,
                                                 &error);
        OCL::checkError(error);
        return gpu_context;
    }

    template <int version>
    static cl_command_queue makeCommandQueue(cl_device_id & device, cl_context & context, const Settings & settings [[maybe_unused]])
    {
        cl_int error;
        cl_command_queue command_queue;

        if constexpr (version == 1)
        {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
            command_queue = clCreateCommandQueue(context, device, settings.command_queue_properties, &error);
#pragma GCC diagnostic pop
        }
        else
        {
#ifdef CL_VERSION_2_0
            command_queue = clCreateCommandQueueWithProperties(context, device, nullptr, &error);
#else
            throw DB::Exception("Binary is built with OpenCL version < 2.0", DB::ErrorCodes::OPENCL_ERROR);
#endif
        }

        OCL::checkError(error);
        return command_queue;
    }

    static cl_program makeProgram(const char * source_code, cl_context context,
                                  cl_device_id device_id, const Settings & settings)
    {
        cl_int error = 0;
        size_t source_size = strlen(source_code);

        cl_program program = clCreateProgramWithSource(context, settings.number_of_program_source_pointers,
                                                       &source_code, &source_size, &error);
        checkError(error);

        error = clBuildProgram(program, settings.number_of_devices_entries, &device_id, settings.build_options,
                               settings.build_notification_routine, settings.build_callback_data);

        /// Combining additional logs output when program build failed.
        if (error == CL_BUILD_PROGRAM_FAILURE)
        {
            size_t log_size;
            error = clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, 0, nullptr, &log_size);

            checkError(error);

            std::vector<char> log(log_size);
            clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, log_size, log.data(), nullptr);

            checkError(error);
            throw DB::Exception(log.data(), DB::ErrorCodes::OPENCL_ERROR);
        }

        checkError(error);
        return program;
    }

    /// Configuring buffer for given input data

    template<typename K>
    static cl_mem createBuffer(K * p_input, cl_int array_size, cl_context context, cl_int elements_size = sizeof(K))
    {
        cl_int error = CL_SUCCESS;
        cl_mem cl_input_buffer = clCreateBuffer(
                                context,
                                CL_MEM_USE_HOST_PTR,
                                zeroCopySizeAlignment(elements_size * array_size),
                                p_input,
                                &error);
        checkError(error);
        return cl_input_buffer;
    }

    static size_t zeroCopySizeAlignment(size_t required_size)
    {
        return required_size + (~required_size + 1) % 64;
    }

    /// Manipulating with common OpenCL variables.

    static void finishCommandQueue(cl_command_queue command_queue)
    {
        // Blocks until all previously queued OpenCL commands in a queue are issued to the associated device.
        cl_int error = clFinish(command_queue);
        OCL::checkError(error);
    }

    template<class T>
    static void releaseData(T * origin, cl_int array_size, cl_mem cl_buffer, cl_command_queue command_queue, size_t offset = 0)
    {
        cl_int error = CL_SUCCESS;

        void * tmp_ptr = nullptr;

        // No events specified to be completed before enqueueing buffers,
        // so `num_events_in_wait_list` passed with `0` value.

        tmp_ptr = clEnqueueMapBuffer(command_queue, cl_buffer, true, CL_MAP_READ,
                                     offset, sizeof(cl_int) * array_size, 0, nullptr, nullptr, &error);
        OCL::checkError(error);
        if (tmp_ptr != origin)
            throw DB::Exception("clEnqueueMapBuffer failed to return original pointer", DB::ErrorCodes::OPENCL_ERROR);

        error = clEnqueueUnmapMemObject(command_queue, cl_buffer, tmp_ptr, 0, nullptr, nullptr);
        checkError(error);

        error = clReleaseMemObject(cl_buffer);
        checkError(error);
    }
};

#endif
