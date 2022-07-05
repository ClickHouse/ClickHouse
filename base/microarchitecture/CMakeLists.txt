# A library with utilities for microarchitecture optimization
add_library(microarchitecture INTERFACE)
target_include_directories(microarchitecture INTERFACE include)
target_include_directories(microarchitecture INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/include")

# Ifunc is only possible with newer glibc on ELF platforms. Another thing to notice is that
# LLVM may not handle ifuncs perfectly. You cannot create aliases or mark the resolver with
# hidden visibility especially when LTO is enabled.
# Issue: https://github.com/llvm/llvm-project/issues/56290
option(MICROARCHITECTURE_ENABLE_IFUNC "Use ifunc to avoid runtime dispatch" OFF)

if(MICROARCHITECTURE_ENABLE_IFUNC)
    if(NOT OS_LINUX OR SANITIZE OR USE_MUSL)
        message(STATUS "Turning off ifunc: it cannot be enabled with target settings")
        set(MICROARCHITECTURE_ENABLE_IFUNC OFF CACHE INTERNAL "Enforcing ifunc to be disabled")
    endif()
endif()

if(ARCH_AARCH64)
    # Check required attributes
    function(check_aarch64_attribute FEATURE_NAME)
        string(TOUPPER ${FEATURE_NAME} UPPER_FEATURE_NAME)
        check_cxx_source_compiles("__attribute__((target(\"${FEATURE_NAME}\"))) int test() { return 0; } int main() { return 0; }" MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_WITHOUT_PREFIX)
        check_cxx_source_compiles("__attribute__((target(\"+${FEATURE_NAME}\"))) int test() { return 0; } int main() { return 0; }" MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_WITH_PREFIX)
        if (${MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_WITHOUT_PREFIX})
            set(MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_ATTRIBUTE "__attribute__((target(\"${FEATURE_NAME}\")))" CACHE INTERNAL "attribute for ${FEATURE_NAME}")
        elseif(${MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_WITH_PREFIX})
            set(MICROARCHITECTURE_AARCH64_${UPPER_FEATURE_NAME}_ATTRIBUTE "__attribute__((target(\"+${FEATURE_NAME}\")))" CACHE INTERNAL "attribute for ${FEATURE_NAME}")
        endif()
    endfunction()

    check_aarch64_attribute(mops)
    check_aarch64_attribute(sve)
    check_aarch64_attribute(sve2)

    if(MICROARCHITECTURE_ENABLE_IFUNC)
        check_cxx_source_compiles("void test() __attribute__((ifunc(\"resolver\"))); extern \"C\" decltype(test)* resolver() { return nullptr; } int main() { return 0; }" MICROARCHITECTURE_AARCH64_IFUNC_SUPPORT)
    endif()

    configure_file(include/microarchitecture/internal/aarch64.config.h.in 
                "${CMAKE_CURRENT_BINARY_DIR}/include/microarchitecture/internal/aarch64.config.h")
endif()

if(ARCH_AMD64)
    # Check required attributes
    function(check_x86_64_level LEVEL_FLAGS LEVEL)
        check_cxx_source_compiles("__attribute__((target(\"${LEVEL_FLAGS}\"))) int test() { return 0; } int main() {}" MICROARCHITECTURE_X86_64_${LEVEL}_SUPPORTED)
        if (${MICROARCHITECTURE_X86_64_${LEVEL}_SUPPORTED})
            set(MICROARCHITECTURE_X86_64_${LEVEL}_ATTRIBUTE "__attribute__((target(\"${LEVEL_FLAGS}\")))" CACHE INTERNAL "attribute for X86_64_${LEVEL}")
        endif()
    endfunction()

    check_x86_64_level(avx,avx2,fma,bmi,bmi2 V3)
    check_x86_64_level(avx512f,avx512vl,avx512bw,avx512cd,avx512dq,avx,avx2,fma,bmi,bmi2 V4)

    if(MICROARCHITECTURE_ENABLE_IFUNC)
        check_cxx_source_compiles("void test() __attribute__((ifunc(\"resolver\"))); extern \"C\" decltype(test)* resolver() { return nullptr; } int main() { return 0; }" MICROARCHITECTURE_X86_64_IFUNC_SUPPORT)
    endif()

    configure_file(include/microarchitecture/internal/x86_64.config.h.in 
                "${CMAKE_CURRENT_BINARY_DIR}/include/microarchitecture/internal/x86_64.config.h")
endif()