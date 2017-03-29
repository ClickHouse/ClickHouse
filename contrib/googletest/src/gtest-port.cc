// Copyright 2008, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Author: wan@google.com (Zhanyong Wan)

#include "gtest/internal/gtest-port.h"

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if GTEST_OS_WINDOWS_MOBILE
# include <windows.h>  // For TerminateProcess()
#elif GTEST_OS_WINDOWS
# include <io.h>
# include <sys/stat.h>
#else
# include <unistd.h>
#endif  // GTEST_OS_WINDOWS_MOBILE

#if GTEST_OS_MAC
# include <mach/mach_init.h>
# include <mach/task.h>
# include <mach/vm_map.h>
#endif  // GTEST_OS_MAC

#if GTEST_OS_QNX
# include <devctl.h>
# include <sys/procfs.h>
#endif  // GTEST_OS_QNX

#include "gtest/gtest-spi.h"
#include "gtest/gtest-message.h"
#include "gtest/internal/gtest-internal.h"
#include "gtest/internal/gtest-string.h"

// Indicates that this translation unit is part of Google Test's
// implementation.  It must come before gtest-internal-inl.h is
// included, or there will be a compiler error.  This trick is to
// prevent a user from accidentally including gtest-internal-inl.h in
// his code.
#define GTEST_IMPLEMENTATION_ 1
#include "src/gtest-internal-inl.h"
#undef GTEST_IMPLEMENTATION_

namespace testing {
namespace internal {

#if defined(_MSC_VER) || defined(__BORLANDC__)
// MSVC and C++Builder do not provide a definition of STDERR_FILENO.
const int kStdOutFileno = 1;
const int kStdErrFileno = 2;
#else
const int kStdOutFileno = STDOUT_FILENO;
const int kStdErrFileno = STDERR_FILENO;
#endif  // _MSC_VER

#if GTEST_OS_MAC

// Returns the number of threads running in the process, or 0 to indicate that
// we cannot detect it.
size_t GetThreadCount() {
  const task_t task = mach_task_self();
  mach_msg_type_number_t thread_count;
  thread_act_array_t thread_list;
  const kern_return_t status = task_threads(task, &thread_list, &thread_count);
  if (status == KERN_SUCCESS) {
    // task_threads allocates resources in thread_list and we need to free them
    // to avoid leaks.
    vm_deallocate(task,
                  reinterpret_cast<vm_address_t>(thread_list),
                  sizeof(thread_t) * thread_count);
    return static_cast<size_t>(thread_count);
  } else {
    return 0;
  }
}

#elif GTEST_OS_QNX

// Returns the number of threads running in the process, or 0 to indicate that
// we cannot detect it.
size_t GetThreadCount() {
  const int fd = open("/proc/self/as", O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  procfs_info process_info;
  const int status =
      devctl(fd, DCMD_PROC_INFO, &process_info, sizeof(process_info), NULL);
  close(fd);
  if (status == EOK) {
    return static_cast<size_t>(process_info.num_threads);
  } else {
    return 0;
  }
}

#else

size_t GetThreadCount() {
  // There's no portable way to detect the number of threads, so we just
  // return 0 to indicate that we cannot detect it.
  return 0;
}

#endif  // GTEST_OS_MAC

#if GTEST_USES_POSIX_RE

// Implements RE.  Currently only needed for death tests.

RE::~RE() {
  if (is_valid_) {
    // regfree'ing an invalid regex might crash because the content
    // of the regex is undefined. Since the regex's are essentially
    // the same, one cannot be valid (or invalid) without the other
    // being so too.
    regfree(&partial_regex_);
    regfree(&full_regex_);
  }
  free(const_cast<char*>(pattern_));
}

// Returns true iff regular expression re matches the entire str.
bool RE::FullMatch(const char* str, const RE& re) {
  if (!re.is_valid_) return false;

  regmatch_t match;
  return regexec(&re.full_regex_, str, 1, &match, 0) == 0;
}

// Returns true iff regular expression re matches a substring of str
// (including str itself).
bool RE::PartialMatch(const char* str, const RE& re) {
  if (!re.is_valid_) return false;

  regmatch_t match;
  return regexec(&re.partial_regex_, str, 1, &match, 0) == 0;
}

// Initializes an RE from its string representation.
void RE::Init(const char* regex) {
  pattern_ = posix::StrDup(regex);

  // Reserves enough bytes to hold the regular expression used for a
  // full match.
  const size_t full_regex_len = strlen(regex) + 10;
  char* const full_pattern = new char[full_regex_len];

  snprintf(full_pattern, full_regex_len, "^(%s)$", regex);
  is_valid_ = regcomp(&full_regex_, full_pattern, REG_EXTENDED) == 0;
  // We want to call regcomp(&partial_regex_, ...) even if the
  // previous expression returns false.  Otherwise partial_regex_ may
  // not be properly initialized can may cause trouble when it's
  // freed.
  //
  // Some implementation of POSIX regex (e.g. on at least some
  // versions of Cygwin) doesn't accept the empty string as a valid
  // regex.  We change it to an equivalent form "()" to be safe.
  if (is_valid_) {
    const char* const partial_regex = (*regex == '\0') ? "()" : regex;
    is_valid_ = regcomp(&partial_regex_, partial_regex, REG_EXTENDED) == 0;
  }
  EXPECT_TRUE(is_valid_)
      << "Regular expression \"" << regex
      << "\" is not a valid POSIX Extended regular expression.";

  delete[] full_pattern;
}

#elif GTEST_USES_SIMPLE_RE

// Returns true iff ch appears anywhere in str (excluding the
// terminating '\0' character).
bool IsInSet(char ch, const char* str) {
  return ch != '\0' && strchr(str, ch) != NULL;
}

// Returns true iff ch belongs to the given classification.  Unlike
// similar functions in <ctype.h>, these aren't affected by the
// current locale.
bool IsAsciiDigit(char ch) { return '0' <= ch && ch <= '9'; }
bool IsAsciiPunct(char ch) {
  return IsInSet(ch, "^-!\"#$%&'()*+,./:;<=>?@[\\]_`{|}~");
}
bool IsRepeat(char ch) { return IsInSet(ch, "?*+"); }
bool IsAsciiWhiteSpace(char ch) { return IsInSet(ch, " \f\n\r\t\v"); }
bool IsAsciiWordChar(char ch) {
  return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') ||
      ('0' <= ch && ch <= '9') || ch == '_';
}

// Returns true iff "\\c" is a supported escape sequence.
bool IsValidEscape(char c) {
  return (IsAsciiPunct(c) || IsInSet(c, "dDfnrsStvwW"));
}

// Returns true iff the given atom (specified by escaped and pattern)
// matches ch.  The result is undefined if the atom is invalid.
bool AtomMatchesChar(bool escaped, char pattern_char, char ch) {
  if (escaped) {  // "\\p" where p is pattern_char.
    switch (pattern_char) {
      case 'd': return IsAsciiDigit(ch);
      case 'D': return !IsAsciiDigit(ch);
      case 'f': return ch == '\f';
      case 'n': return ch == '\n';
      case 'r': return ch == '\r';
      case 's': return IsAsciiWhiteSpace(ch);
      case 'S': return !IsAsciiWhiteSpace(ch);
      case 't': return ch == '\t';
      case 'v': return ch == '\v';
      case 'w': return IsAsciiWordChar(ch);
      case 'W': return !IsAsciiWordChar(ch);
    }
    return IsAsciiPunct(pattern_char) && pattern_char == ch;
  }

  return (pattern_char == '.' && ch != '\n') || pattern_char == ch;
}

// Helper function used by ValidateRegex() to format error messages.
std::string FormatRegexSyntaxError(const char* regex, int index) {
  return (Message() << "Syntax error at index " << index
          << " in simple regular expression \"" << regex << "\": ").GetString();
}

// Generates non-fatal failures and returns false if regex is invalid;
// otherwise returns true.
bool ValidateRegex(const char* regex) {
  if (regex == NULL) {
    // TODO(wan@google.com): fix the source file location in the
    // assertion failures to match where the regex is used in user
    // code.
    ADD_FAILURE() << "NULL is not a valid simple regular expression.";
    return false;
  }

  bool is_valid = true;

  // True iff ?, *, or + can follow the previous atom.
  bool prev_repeatable = false;
  for (int i = 0; regex[i]; i++) {
    if (regex[i] == '\\') {  // An escape sequence
      i++;
      if (regex[i] == '\0') {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i - 1)
                      << "'\\' cannot appear at the end.";
        return false;
      }

      if (!IsValidEscape(regex[i])) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i - 1)
                      << "invalid escape sequence \"\\" << regex[i] << "\".";
        is_valid = false;
      }
      prev_repeatable = true;
    } else {  // Not an escape sequence.
      const char ch = regex[i];

      if (ch == '^' && i > 0) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'^' can only appear at the beginning.";
        is_valid = false;
      } else if (ch == '$' && regex[i + 1] != '\0') {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'$' can only appear at the end.";
        is_valid = false;
      } else if (IsInSet(ch, "()[]{}|")) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'" << ch << "' is unsupported.";
        is_valid = false;
      } else if (IsRepeat(ch) && !prev_repeatable) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'" << ch << "' can only follow a repeatable token.";
        is_valid = false;
      }

      prev_repeatable = !IsInSet(ch, "^$?*+");
    }
  }

  return is_valid;
}

// Matches a repeated regex atom followed by a valid simple regular
// expression.  The regex atom is defined as c if escaped is false,
// or \c otherwise.  repeat is the repetition meta character (?, *,
// or +).  The behavior is undefined if str contains too many
// characters to be indexable by size_t, in which case the test will
// probably time out anyway.  We are fine with this limitation as
// std::string has it too.
bool MatchRepetitionAndRegexAtHead(
    bool escaped, char c, char repeat, const char* regex,
    const char* str) {
  const size_t min_count = (repeat == '+') ? 1 : 0;
  const size_t max_count = (repeat == '?') ? 1 :
      static_cast<size_t>(-1) - 1;
  // We cannot call numeric_limits::max() as it conflicts with the
  // max() macro on Windows.

  for (size_t i = 0; i <= max_count; ++i) {
    // We know that the atom matches each of the first i characters in str.
    if (i >= min_count && MatchRegexAtHead(regex, str + i)) {
      // We have enough matches at the head, and the tail matches too.
      // Since we only care about *whether* the pattern matches str
      // (as opposed to *how* it matches), there is no need to find a
      // greedy match.
      return true;
    }
    if (str[i] == '\0' || !AtomMatchesChar(escaped, c, str[i]))
      return false;
  }
  return false;
}

// Returns true iff regex matches a prefix of str.  regex must be a
// valid simple regular expression and not start with "^", or the
// result is undefined.
bool MatchRegexAtHead(const char* regex, const char* str) {
  if (*regex == '\0')  // An empty regex matches a prefix of anything.
    return true;

  // "$" only matches the end of a string.  Note that regex being
  // valid guarantees that there's nothing after "$" in it.
  if (*regex == '$')
    return *str == '\0';

  // Is the first thing in regex an escape sequence?
  const bool escaped = *regex == '\\';
  if (escaped)
    ++regex;
  if (IsRepeat(regex[1])) {
    // MatchRepetitionAndRegexAtHead() calls MatchRegexAtHead(), so
    // here's an indirect recursion.  It terminates as the regex gets
    // shorter in each recursion.
    return MatchRepetitionAndRegexAtHead(
        escaped, regex[0], regex[1], regex + 2, str);
  } else {
    // regex isn't empty, isn't "$", and doesn't start with a
    // repetition.  We match the first atom of regex with the first
    // character of str and recurse.
    return (*str != '\0') && AtomMatchesChar(escaped, *regex, *str) &&
        MatchRegexAtHead(regex + 1, str + 1);
  }
}

// Returns true iff regex matches any substring of str.  regex must be
// a valid simple regular expression, or the result is undefined.
//
// The algorithm is recursive, but the recursion depth doesn't exceed
// the regex length, so we won't need to worry about running out of
// stack space normally.  In rare cases the time complexity can be
// exponential with respect to the regex length + the string length,
// but usually it's must faster (often close to linear).
bool MatchRegexAnywhere(const char* regex, const char* str) {
  if (regex == NULL || str == NULL)
    return false;

  if (*regex == '^')
    return MatchRegexAtHead(regex + 1, str);

  // A successful match can be anywhere in str.
  do {
    if (MatchRegexAtHead(regex, str))
      return true;
  } while (*str++ != '\0');
  return false;
}

// Implements the RE class.

RE::~RE() {
  free(const_cast<char*>(pattern_));
  free(const_cast<char*>(full_pattern_));
}

// Returns true iff regular expression re matches the entire str.
bool RE::FullMatch(const char* str, const RE& re) {
  return re.is_valid_ && MatchRegexAnywhere(re.full_pattern_, str);
}

// Returns true iff regular expression re matches a substring of str
// (including str itself).
bool RE::PartialMatch(const char* str, const RE& re) {
  return re.is_valid_ && MatchRegexAnywhere(re.pattern_, str);
}

// Initializes an RE from its string representation.
void RE::Init(const char* regex) {
  pattern_ = full_pattern_ = NULL;
  if (regex != NULL) {
    pattern_ = posix::StrDup(regex);
  }

  is_valid_ = ValidateRegex(regex);
  if (!is_valid_) {
    // No need to calculate the full pattern when the regex is invalid.
    return;
  }

  const size_t len = strlen(regex);
  // Reserves enough bytes to hold the regular expression used for a
  // full match: we need space to prepend a '^', append a '$', and
  // terminate the string with '\0'.
  char* buffer = static_cast<char*>(malloc(len + 3));
  full_pattern_ = buffer;

  if (*regex != '^')
    *buffer++ = '^';  // Makes sure full_pattern_ starts with '^'.

  // We don't use snprintf or strncpy, as they trigger a warning when
  // compiled with VC++ 8.0.
  memcpy(buffer, regex, len);
  buffer += len;

  if (len == 0 || regex[len - 1] != '$')
    *buffer++ = '$';  // Makes sure full_pattern_ ends with '$'.

  *buffer = '\0';
}

#endif  // GTEST_USES_POSIX_RE

const char kUnknownFile[] = "unknown file";

// Formats a source file path and a line number as they would appear
// in an error message from the compiler used to compile this code.
GTEST_API_ ::std::string FormatFileLocation(const char* file, int line) {
  const std::string file_name(file == NULL ? kUnknownFile : file);

  if (line < 0) {
    return file_name + ":";
  }
#ifdef _MSC_VER
  return file_name + "(" + StreamableToString(line) + "):";
#else
  return file_name + ":" + StreamableToString(line) + ":";
#endif  // _MSC_VER
}

// Formats a file location for compiler-independent XML output.
// Although this function is not platform dependent, we put it next to
// FormatFileLocation in order to contrast the two functions.
// Note that FormatCompilerIndependentFileLocation() does NOT append colon
// to the file location it produces, unlike FormatFileLocation().
GTEST_API_ ::std::string FormatCompilerIndependentFileLocation(
    const char* file, int line) {
  const std::string file_name(file == NULL ? kUnknownFile : file);

  if (line < 0)
    return file_name;
  else
    return file_name + ":" + StreamableToString(line);
}


GTestLog::GTestLog(GTestLogSeverity severity, const char* file, int line)
    : severity_(severity) {
  const char* const marker =
      severity == GTEST_INFO ?    "[  INFO ]" :
      severity == GTEST_WARNING ? "[WARNING]" :
      severity == GTEST_ERROR ?   "[ ERROR ]" : "[ FATAL ]";
  GetStream() << ::std::endl << marker << " "
              << FormatFileLocation(file, line).c_str() << ": ";
}

// Flushes the buffers and, if severity is GTEST_FATAL, aborts the program.
GTestLog::~GTestLog() {
  GetStream() << ::std::endl;
  if (severity_ == GTEST_FATAL) {
    fflush(stderr);
    posix::Abort();
  }
}
// Disable Microsoft deprecation warnings for POSIX functions called from
// this class (creat, dup, dup2, and close)
#ifdef _MSC_VER
# pragma warning(push)
# pragma warning(disable: 4996)
#endif  // _MSC_VER

#if GTEST_HAS_STREAM_REDIRECTION

// Object that captures an output stream (stdout/stderr).
class CapturedStream {
 public:
  // The ctor redirects the stream to a temporary file.
  explicit CapturedStream(int fd) : fd_(fd), uncaptured_fd_(dup(fd)) {
# if GTEST_OS_WINDOWS
    char temp_dir_path[MAX_PATH + 1] = { '\0' };  // NOLINT
    char temp_file_path[MAX_PATH + 1] = { '\0' };  // NOLINT

    ::GetTempPathA(sizeof(temp_dir_path), temp_dir_path);
    const UINT success = ::GetTempFileNameA(temp_dir_path,
                                            "gtest_redir",
                                            0,  // Generate unique file name.
                                            temp_file_path);
    GTEST_CHECK_(success != 0)
        << "Unable to create a temporary file in " << temp_dir_path;
    const int captured_fd = creat(temp_file_path, _S_IREAD | _S_IWRITE);
    GTEST_CHECK_(captured_fd != -1) << "Unable to open temporary file "
                                    << temp_file_path;
    filename_ = temp_file_path;
# else
    // There's no guarantee that a test has write access to the current
    // directory, so we create the temporary file in the /tmp directory
    // instead. We use /tmp on most systems, and /sdcard on Android.
    // That's because Android doesn't have /tmp.
#  if GTEST_OS_LINUX_ANDROID
    // Note: Android applications are expected to call the framework's
    // Context.getExternalStorageDirectory() method through JNI to get
    // the location of the world-writable SD Card directory. However,
    // this requires a Context handle, which cannot be retrieved
    // globally from native code. Doing so also precludes running the
    // code as part of a regular standalone executable, which doesn't
    // run in a Dalvik process (e.g. when running it through 'adb shell').
    //
    // The location /sdcard is directly accessible from native code
    // and is the only location (unofficially) supported by the Android
    // team. It's generally a symlink to the real SD Card mount point
    // which can be /mnt/sdcard, /mnt/sdcard0, /system/media/sdcard, or
    // other OEM-customized locations. Never rely on these, and always
    // use /sdcard.
    char name_template[] = "/sdcard/gtest_captured_stream.XXXXXX";
#  else
    char name_template[] = "/tmp/captured_stream.XXXXXX";
#  endif  // GTEST_OS_LINUX_ANDROID
    const int captured_fd = mkstemp(name_template);
    filename_ = name_template;
# endif  // GTEST_OS_WINDOWS
    fflush(NULL);
    dup2(captured_fd, fd_);
    close(captured_fd);
  }

  ~CapturedStream() {
    remove(filename_.c_str());
  }

  std::string GetCapturedString() {
    if (uncaptured_fd_ != -1) {
      // Restores the original stream.
      fflush(NULL);
      dup2(uncaptured_fd_, fd_);
      close(uncaptured_fd_);
      uncaptured_fd_ = -1;
    }

    FILE* const file = posix::FOpen(filename_.c_str(), "r");
    const std::string content = ReadEntireFile(file);
    posix::FClose(file);
    return content;
  }

 private:
  // Reads the entire content of a file as an std::string.
  static std::string ReadEntireFile(FILE* file);

  // Returns the size (in bytes) of a file.
  static size_t GetFileSize(FILE* file);

  const int fd_;  // A stream to capture.
  int uncaptured_fd_;
  // Name of the temporary file holding the stderr output.
  ::std::string filename_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(CapturedStream);
};

// Returns the size (in bytes) of a file.
size_t CapturedStream::GetFileSize(FILE* file) {
  fseek(file, 0, SEEK_END);
  return static_cast<size_t>(ftell(file));
}

// Reads the entire content of a file as a string.
std::string CapturedStream::ReadEntireFile(FILE* file) {
  const size_t file_size = GetFileSize(file);
  char* const buffer = new char[file_size];

  size_t bytes_last_read = 0;  // # of bytes read in the last fread()
  size_t bytes_read = 0;       // # of bytes read so far

  fseek(file, 0, SEEK_SET);

  // Keeps reading the file until we cannot read further or the
  // pre-determined file size is reached.
  do {
    bytes_last_read = fread(buffer+bytes_read, 1, file_size-bytes_read, file);
    bytes_read += bytes_last_read;
  } while (bytes_last_read > 0 && bytes_read < file_size);

  const std::string content(buffer, bytes_read);
  delete[] buffer;

  return content;
}

# ifdef _MSC_VER
#  pragma warning(pop)
# endif  // _MSC_VER

static CapturedStream* g_captured_stderr = NULL;
static CapturedStream* g_captured_stdout = NULL;

// Starts capturing an output stream (stdout/stderr).
void CaptureStream(int fd, const char* stream_name, CapturedStream** stream) {
  if (*stream != NULL) {
    GTEST_LOG_(FATAL) << "Only one " << stream_name
                      << " capturer can exist at a time.";
  }
  *stream = new CapturedStream(fd);
}

// Stops capturing the output stream and returns the captured string.
std::string GetCapturedStream(CapturedStream** captured_stream) {
  const std::string content = (*captured_stream)->GetCapturedString();

  delete *captured_stream;
  *captured_stream = NULL;

  return content;
}

// Starts capturing stdout.
void CaptureStdout() {
  CaptureStream(kStdOutFileno, "stdout", &g_captured_stdout);
}

// Starts capturing stderr.
void CaptureStderr() {
  CaptureStream(kStdErrFileno, "stderr", &g_captured_stderr);
}

// Stops capturing stdout and returns the captured string.
std::string GetCapturedStdout() {
  return GetCapturedStream(&g_captured_stdout);
}

// Stops capturing stderr and returns the captured string.
std::string GetCapturedStderr() {
  return GetCapturedStream(&g_captured_stderr);
}

#endif  // GTEST_HAS_STREAM_REDIRECTION

#if GTEST_HAS_DEATH_TEST

// A copy of all command line arguments.  Set by InitGoogleTest().
::std::vector<testing::internal::string> g_argvs;

static const ::std::vector<testing::internal::string>* g_injected_test_argvs =
                                        NULL;  // Owned.

void SetInjectableArgvs(const ::std::vector<testing::internal::string>* argvs) {
  if (g_injected_test_argvs != argvs)
    delete g_injected_test_argvs;
  g_injected_test_argvs = argvs;
}

const ::std::vector<testing::internal::string>& GetInjectableArgvs() {
  if (g_injected_test_argvs != NULL) {
    return *g_injected_test_argvs;
  }
  return g_argvs;
}
#endif  // GTEST_HAS_DEATH_TEST

#if GTEST_OS_WINDOWS_MOBILE
namespace posix {
void Abort() {
  DebugBreak();
  TerminateProcess(GetCurrentProcess(), 1);
}
}  // namespace posix
#endif  // GTEST_OS_WINDOWS_MOBILE

// Returns the name of the environment variable corresponding to the
// given flag.  For example, FlagToEnvVar("foo") will return
// "GTEST_FOO" in the open-source version.
static std::string FlagToEnvVar(const char* flag) {
  const std::string full_flag =
      (Message() << GTEST_FLAG_PREFIX_ << flag).GetString();

  Message env_var;
  for (size_t i = 0; i != full_flag.length(); i++) {
    env_var << ToUpper(full_flag.c_str()[i]);
  }

  return env_var.GetString();
}

// Parses 'str' for a 32-bit signed integer.  If successful, writes
// the result to *value and returns true; otherwise leaves *value
// unchanged and returns false.
bool ParseInt32(const Message& src_text, const char* str, Int32* value) {
  // Parses the environment variable as a decimal integer.
  char* end = NULL;
  const long long_value = strtol(str, &end, 10);  // NOLINT

  // Has strtol() consumed all characters in the string?
  if (*end != '\0') {
    // No - an invalid character was encountered.
    Message msg;
    msg << "WARNING: " << src_text
        << " is expected to be a 32-bit integer, but actually"
        << " has value \"" << str << "\".\n";
    printf("%s", msg.GetString().c_str());
    fflush(stdout);
    return false;
  }

  // Is the parsed value in the range of an Int32?
  const Int32 result = static_cast<Int32>(long_value);
  if (long_value == LONG_MAX || long_value == LONG_MIN ||
      // The parsed value overflows as a long.  (strtol() returns
      // LONG_MAX or LONG_MIN when the input overflows.)
      result != long_value
      // The parsed value overflows as an Int32.
      ) {
    Message msg;
    msg << "WARNING: " << src_text
        << " is expected to be a 32-bit integer, but actually"
        << " has value " << str << ", which overflows.\n";
    printf("%s", msg.GetString().c_str());
    fflush(stdout);
    return false;
  }

  *value = result;
  return true;
}

// Reads and returns the Boolean environment variable corresponding to
// the given flag; if it's not set, returns default_value.
//
// The value is considered true iff it's not "0".
bool BoolFromGTestEnv(const char* flag, bool default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const string_value = posix::GetEnv(env_var.c_str());
  return string_value == NULL ?
      default_value : strcmp(string_value, "0") != 0;
}

// Reads and returns a 32-bit integer stored in the environment
// variable corresponding to the given flag; if it isn't set or
// doesn't represent a valid 32-bit integer, returns default_value.
Int32 Int32FromGTestEnv(const char* flag, Int32 default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const string_value = posix::GetEnv(env_var.c_str());
  if (string_value == NULL) {
    // The environment variable is not set.
    return default_value;
  }

  Int32 result = default_value;
  if (!ParseInt32(Message() << "Environment variable " << env_var,
                  string_value, &result)) {
    printf("The default value %s is used.\n",
           (Message() << default_value).GetString().c_str());
    fflush(stdout);
    return default_value;
  }

  return result;
}

// Reads and returns the string environment variable corresponding to
// the given flag; if it's not set, returns default_value.
const char* StringFromGTestEnv(const char* flag, const char* default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const value = posix::GetEnv(env_var.c_str());
  return value == NULL ? default_value : value;
}

}  // namespace internal
}  // namespace testing
