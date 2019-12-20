# Not changed part of contrib/orc/c++/src/CMakeLists.txt

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

# See https://cmake.org/cmake/help/v3.14/policy/CMP0075.html. Without unsetting it breaks thrift.
set(CMAKE_REQUIRED_INCLUDES)
set(CMAKE_REQUIRED_LIBRARIES)
