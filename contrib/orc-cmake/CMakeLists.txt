# modifyed copy of contrib/orc/c++/src/CMakeLists.txt
set(LIBRARY_INCLUDE ${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include)
set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/orc/c++/src)

set(PROTOBUF_INCLUDE_DIR ${Protobuf_INCLUDE_DIR})
set(PROTOBUF_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

INCLUDE(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
    #include<fcntl.h>
    #include<unistd.h>
    int main(int,char*[]){
      int f = open(\"/x/y\", O_RDONLY);
      char buf[100];
      return pread(f, buf, 100, 1000) == 0;
    }"
  HAS_PREAD
)

CHECK_CXX_SOURCE_COMPILES("
    #include<time.h>
    int main(int,char*[]){
      struct tm time2020;
      return !strptime(\"2020-02-02 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2020);
    }"
  HAS_STRPTIME
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    int main(int,char* argv[]){
      return static_cast<int>(std::stoll(argv[0]));
    }"
  HAS_STOLL
)

CHECK_CXX_SOURCE_COMPILES("
    #include<stdint.h>
    #include<stdio.h>
    int main(int,char*[]){
      int64_t x = 1; printf(\"%lld\",x);
    }"
  INT64_IS_LL
)

CHECK_CXX_SOURCE_COMPILES("
    #ifdef __clang__
      #pragma clang diagnostic push
      #pragma clang diagnostic ignored \"-Wdeprecated\"
      #pragma clang diagnostic pop
   #elif defined(__GNUC__)
      #pragma GCC diagnostic push
      #pragma GCC diagnostic ignored \"-Wdeprecated\"
      #pragma GCC diagnostic pop
   #elif defined(_MSC_VER)
      #pragma warning( push )
      #pragma warning( disable : 4996 )
      #pragma warning( pop )
   #else
     unknownCompiler!
   #endif
   int main(int, char *[]) {}"
  HAS_DIAGNOSTIC_PUSH
)

CHECK_CXX_SOURCE_COMPILES("
    #include<cmath>
    int main(int, char *[]) {
      return std::isnan(1.0f);
    }"
  HAS_STD_ISNAN
)

CHECK_CXX_SOURCE_COMPILES("
    #include<mutex>
    int main(int, char *[]) {
       std::mutex test_mutex;
       std::lock_guard<std::mutex> lock_mutex(test_mutex);
    }"
  HAS_STD_MUTEX
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    std::string func() {
      std::string var = \"test\";
      return std::move(var);
    }
    int main(int, char *[]) {}"
  NEEDS_REDUNDANT_MOVE
)

INCLUDE(CheckCXXSourceRuns)

CHECK_CXX_SOURCE_RUNS("
    #include<time.h>
    int main(int, char *[]) {
      time_t t = -14210715; // 1969-07-20 12:34:45
      struct tm *ptm = gmtime(&t);
      return !(ptm && ptm->tm_year == 69);
    }"
  HAS_PRE_1970
)

CHECK_CXX_SOURCE_RUNS("
    #include<stdlib.h>
    #include<time.h>
    int main(int, char *[]) {
      setenv(\"TZ\", \"America/Los_Angeles\", 1);
      tzset();
      struct tm time2037;
      struct tm time2038;
      strptime(\"2037-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2037);
      strptime(\"2038-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2038);
      return mktime(&time2038) - mktime(&time2037) != 31536000;
    }"
  HAS_POST_2038
)

set(CMAKE_REQUIRED_INCLUDES ${ZLIB_INCLUDE_DIR})
set(CMAKE_REQUIRED_LIBRARIES zlib)
CHECK_CXX_SOURCE_COMPILES("
    #define Z_PREFIX
    #include<zlib.h>
    z_stream strm;
    int main(int, char *[]) {
        deflateReset(&strm);
    }"
  NEEDS_Z_PREFIX
)

configure_file (
  "${LIBRARY_DIR}/Adaptor.hh.in"
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
  )


add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/orc_proto.pb.h ${CMAKE_CURRENT_BINARY_DIR}/orc_proto.pb.cc
   COMMAND ${PROTOBUF_EXECUTABLE}
        -I${ClickHouse_SOURCE_DIR}/contrib/orc/proto
        --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
        "${ClickHouse_SOURCE_DIR}/contrib/orc/proto/orc_proto.proto"
)

set(SOURCE_FILES
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
  ${CMAKE_CURRENT_BINARY_DIR}/orc_proto.pb.h
  ${LIBRARY_DIR}/io/InputStream.cc
  ${LIBRARY_DIR}/io/OutputStream.cc
  ${LIBRARY_DIR}/wrap/orc-proto-wrapper.cc
  ${LIBRARY_DIR}/Adaptor.cc
  ${LIBRARY_DIR}/ByteRLE.cc
  ${LIBRARY_DIR}/ColumnPrinter.cc
  ${LIBRARY_DIR}/ColumnReader.cc
  ${LIBRARY_DIR}/ColumnWriter.cc
  ${LIBRARY_DIR}/Common.cc
  ${LIBRARY_DIR}/Compression.cc
  ${LIBRARY_DIR}/Exceptions.cc
  ${LIBRARY_DIR}/Int128.cc
  ${LIBRARY_DIR}/LzoDecompressor.cc
  ${LIBRARY_DIR}/MemoryPool.cc
  ${LIBRARY_DIR}/OrcFile.cc
  ${LIBRARY_DIR}/Reader.cc
  ${LIBRARY_DIR}/RLEv1.cc
  ${LIBRARY_DIR}/RLEv2.cc
  ${LIBRARY_DIR}/RLE.cc
  ${LIBRARY_DIR}/Statistics.cc
  ${LIBRARY_DIR}/StripeStream.cc
  ${LIBRARY_DIR}/Timezone.cc
  ${LIBRARY_DIR}/TypeImpl.cc
  ${LIBRARY_DIR}/Vector.cc
  ${LIBRARY_DIR}/Writer.cc
  )

if(ORC_CXX_HAS_THREAD_LOCAL AND BUILD_LIBHDFSPP)
  set(SOURCE_FILES ${SOURCE_FILES} ${LIBRARY_DIR}/OrcHdfsFile.cc)
endif(ORC_CXX_HAS_THREAD_LOCAL AND BUILD_LIBHDFSPP)

#list(TRANSFORM SOURCE_FILES PREPEND ${LIBRARY_DIR}/)

configure_file (
  "${LIBRARY_INCLUDE}/orc/orc-config.hh.in"
  "${CMAKE_CURRENT_BINARY_DIR}/orc/orc-config.hh"
  )

add_library (orc ${SOURCE_FILES})

target_include_directories (orc
  PRIVATE
  ${LIBRARY_INCLUDE}
  ${LIBRARY_DIR}
  #PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE
  ${PROTOBUF_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIR}
  ${SNAPPY_INCLUDE_DIR}
  ${LZ4_INCLUDE_DIR}
  ${LIBHDFSPP_INCLUDE_DIR}
  )

target_link_libraries (orc PRIVATE
  ${Protobuf_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${SNAPPY_LIBRARY}
  ${LZ4_LIBRARY}
  ${LIBHDFSPP_LIBRARIES}
  )

#install(TARGETS orc DESTINATION lib)

if(ORC_CXX_HAS_THREAD_LOCAL AND BUILD_LIBHDFSPP)
  add_definitions(-DBUILD_LIBHDFSPP)
endif(ORC_CXX_HAS_THREAD_LOCAL AND BUILD_LIBHDFSPP)
