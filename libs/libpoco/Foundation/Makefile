#
# Makefile
#
# $Id: //poco/1.4/Foundation/Makefile#4 $
#
# Makefile for Poco Foundation
#

include $(POCO_BASE)/build/rules/global

objects = ArchiveStrategy Ascii ASCIIEncoding AsyncChannel \
	Base32Decoder Base32Encoder Base64Decoder Base64Encoder \
	BinaryReader BinaryWriter Bugcheck ByteOrder Channel Checksum Clock Configurable ConsoleChannel \
	Condition CountingStream DateTime LocalDateTime DateTimeFormat DateTimeFormatter DateTimeParser \
	Debugger DeflatingStream DigestEngine DigestStream DirectoryIterator DirectoryWatcher \
	Environment Event Error EventArgs ErrorHandler Exception FIFOBufferStream FPEnvironment File \
	FileChannel Formatter FormattingChannel Glob HexBinaryDecoder LineEndingConverter \
	HexBinaryEncoder InflatingStream Latin1Encoding Latin2Encoding Latin9Encoding LogFile \
	Logger LoggingFactory LoggingRegistry LogStream NamedEvent NamedMutex NullChannel \
	MemoryPool MD4Engine MD5Engine Manifest Message Mutex \
	NestedDiagnosticContext Notification NotificationCenter \
	NotificationQueue PriorityNotificationQueue TimedNotificationQueue \
	NullStream NumberFormatter NumberParser NumericString AbstractObserver \
	Path PatternFormatter Process PurgeStrategy RWLock Random RandomStream \
	DirectoryIteratorStrategy RegularExpression RefCountedObject Runnable RotateStrategy \
	SHA1Engine Semaphore SharedLibrary SimpleFileChannel \
	SignalHandler SplitterChannel SortedDirectoryIterator Stopwatch StreamChannel \
	StreamConverter StreamCopier StreamTokenizer String StringTokenizer SynchronizedObject \
	Task TaskManager TaskNotification TeeStream Hash HashStatistic \
	TemporaryFile TextConverter TextEncoding TextIterator TextBufferIterator Thread ThreadLocal \
	ThreadPool ThreadTarget ActiveDispatcher Timer Timespan Timestamp Timezone Token URI \
	FileStreamFactory URIStreamFactory URIStreamOpener UTF32Encoding UTF16Encoding UTF8Encoding UTF8String \
	Unicode UnicodeConverter Windows1250Encoding Windows1251Encoding Windows1252Encoding \
	UUID UUIDGenerator Void Var VarHolder VarIterator Format Pipe PipeImpl PipeStream SharedMemory \
	MemoryStream FileStream AtomicCounter 

zlib_objects = adler32 compress crc32 deflate \
	infback inffast inflate inftrees trees zutil

pcre_objects = pcre_config pcre_chartables pcre_compile pcre_globals pcre_maketables \
	pcre_study pcre_byte_order pcre_valid_utf8 pcre_dfa_exec pcre_get pcre_jit_compile\
	pcre_exec pcre_ord2utf8 pcre_newline pcre_fullinfo pcre_xclass pcre_refcount pcre_string_utils \
	pcre_version

pcre_utf8_objects = pcre_ucd pcre_tables

ifdef POCO_UNBUNDLED
	SYSLIBS += -lpcre -lz
else
	objects += $(zlib_objects) $(pcre_objects) $(pcre_utf8_objects)
endif

ifeq ($(findstring MinGW, $(POCO_CONFIG)), MinGW)
	objects += EventLogChannel WindowsConsoleChannel
else
	objects += SyslogChannel
endif

target         = PocoFoundation
target_version = $(LIBVERSION)
target_libs    =

ifeq ($(findstring MinGW, $(POCO_CONFIG)), MinGW)
    $(shell cd src; $(WINDMC) pocomsg.mc)
endif

include $(POCO_BASE)/build/rules/lib
