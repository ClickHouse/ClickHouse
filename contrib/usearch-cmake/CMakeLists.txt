set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/include)

target_link_libraries(_usearch INTERFACE _fp16)
target_compile_definitions(_usearch INTERFACE USEARCH_USE_FP16LIB)

# Only x86 for now. On ARM, the linker goes down in flames. To make SimSIMD compile, I had to remove a macro checks in SimSIMD
# for AVX512 (x86, worked nicely) and __ARM_BF16_FORMAT_ALTERNATIVE. It is probably because of that.
if (ARCH_AMD64)
    target_link_libraries(_usearch INTERFACE _simsimd)
    target_compile_definitions(_usearch INTERFACE USEARCH_USE_SIMSIMD)

    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_FLOAT16)
    target_compile_definitions(_usearch INTERFACE USEARCH_CAN_COMPILE_BF16)
endif ()

add_library(ch_contrib::usearch ALIAS _usearch)


# Cf. https://github.com/llvm/llvm-project/issues/107810 (though it is not 100% the same stack)
#
# LLVM ERROR: Cannot select: 0x7996e7a73150: f32,ch = load<(load (s16) from %ir.22, !tbaa !54231), anyext from bf16> 0x79961cb737c0, 0x7996e7a1a500, undef:i64, ./contrib/SimSIMD/include/simsimd/dot.h:215:1
#   0x7996e7a1a500: i64 = add 0x79961e770d00, Constant:i64<-16>, ./contrib/SimSIMD/include/simsimd/dot.h:215:1
#     0x79961e770d00: i64,ch = CopyFromReg 0x79961cb737c0, Register:i64 %4, ./contrib/SimSIMD/include/simsimd/dot.h:215:1
#       0x7996e7a1ae10: i64 = Register %4
#     0x7996e7a1b5f0: i64 = Constant<-16>
#   0x7996e7a1a730: i64 = undef
# In function: _ZL23simsimd_dot_bf16_serialPKu6__bf16S0_yPd
# PLEASE submit a bug report to https://github.com/llvm/llvm-project/issues/ and include the crash backtrace.
# Stack dump:
# 0.      Running pass 'Function Pass Manager' on module 'src/libdbms.a(MergeTreeIndexVectorSimilarity.cpp.o at 2312737440)'.
# 1.      Running pass 'AArch64 Instruction Selection' on function '@_ZL23simsimd_dot_bf16_serialPKu6__bf16S0_yPd'
#  #0 0x00007999e83a63bf llvm::sys::PrintStackTrace(llvm::raw_ostream&, int) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xda63bf)
#  #1 0x00007999e83a44f9 llvm::sys::RunSignalHandlers() (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xda44f9)
#  #2 0x00007999e83a6b00 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xda6b00)
#  #3 0x00007999e6e45320 (/lib/x86_64-linux-gnu/libc.so.6+0x45320)
#  #4 0x00007999e6e9eb1c pthread_kill (/lib/x86_64-linux-gnu/libc.so.6+0x9eb1c)
#  #5 0x00007999e6e4526e raise (/lib/x86_64-linux-gnu/libc.so.6+0x4526e)
#  #6 0x00007999e6e288ff abort (/lib/x86_64-linux-gnu/libc.so.6+0x288ff)
#  #7 0x00007999e82fe0c2 llvm::report_fatal_error(llvm::Twine const&, bool) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xcfe0c2)
#  #8 0x00007999e8c2f8e3 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x162f8e3)
#  #9 0x00007999e8c2ed76 llvm::SelectionDAGISel::SelectCodeCommon(llvm::SDNode*, unsigned char const*, unsigned int) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x162ed76)
# #10 0x00007999ea1adbcb (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x2badbcb)
# #11 0x00007999e8c2611f llvm::SelectionDAGISel::DoInstructionSelection() (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x162611f)
# #12 0x00007999e8c25790 llvm::SelectionDAGISel::CodeGenAndEmitDAG() (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x1625790)
# #13 0x00007999e8c248de llvm::SelectionDAGISel::SelectAllBasicBlocks(llvm::Function const&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x16248de)
# #14 0x00007999e8c22934 llvm::SelectionDAGISel::runOnMachineFunction(llvm::MachineFunction&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x1622934)
# #15 0x00007999e87826b9 llvm::MachineFunctionPass::runOnFunction(llvm::Function&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x11826b9)
# #16 0x00007999e84f7772 llvm::FPPassManager::runOnFunction(llvm::Function&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xef7772)
# #17 0x00007999e84fd2f4 llvm::FPPassManager::runOnModule(llvm::Module&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xefd2f4)
# #18 0x00007999e84f7e9f llvm::legacy::PassManagerImpl::run(llvm::Module&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xef7e9f)
# #19 0x00007999e99f7d61 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x23f7d61)
# #20 0x00007999e99f8c91 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x23f8c91)
# #21 0x00007999e99f8b10 llvm::lto::thinBackend(llvm::lto::Config const&, unsigned int, std::function<llvm::Expected<std::unique_ptr<llvm::CachedFileStream, std::default_delete<llvm::CachedFileStream>>> (unsigned int, llvm::Twine const&)>, llvm::Module&, llvm::ModuleSummaryIndex const&, llvm::DenseMap<llvm::StringRef, std::unordered_set<unsigned long, std::hash<unsigned long>, std::equal_to<unsigned long>, std::allocator<unsigned long>>, llvm::DenseMapInfo<llvm::StringRef, void
# >, llvm::detail::DenseMapPair<llvm::StringRef, std::unordered_set<unsigned long, std::hash<unsigned long>, std::equal_to<unsigned long>, std::allocator<unsigned long>>>> const&, llvm::DenseMap<unsigned long, llvm::GlobalValueSummary*, llvm::DenseMapInfo<unsigned long, void>, llvm::detail::DenseMapPair<unsigned long, llvm::GlobalValueSummary*>> const&, llvm::MapVector<llvm::StringRef, llvm::BitcodeModule, llvm::DenseMap<llvm::StringRef, unsigned int, llvm::DenseMapInfo<llvm::S
# tringRef, void>, llvm::detail::DenseMapPair<llvm::StringRef, unsigned int>>, llvm::SmallVector<std::pair<llvm::StringRef, llvm::BitcodeModule>, 0u>>*, std::vector<unsigned char, std::allocator<unsigned char>> const&) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x23f8b10)
# #22 0x00007999e99f248d (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x23f248d)
# #23 0x00007999e99f1cd6 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0x23f1cd6)
# #24 0x00007999e82c9beb (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xcc9beb)
# #25 0x00007999e834ebe3 llvm::ThreadPool::processTasks(llvm::ThreadPoolTaskGroup*) (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xd4ebe3)
# #26 0x00007999e834f704 (/usr/lib/llvm-18/bin/../lib/libLLVM.so.18.1+0xd4f704)
# #27 0x00007999e6e9ca94 (/lib/x86_64-linux-gnu/libc.so.6+0x9ca94)
# #28 0x00007999e6f29c3c (/lib/x86_64-linux-gnu/libc.so.6+0x129c3c)
# clang++-18: error: unable to execute command: Aborted (core dumped)
# clang++-18: error: linker command failed due to signal (use -v to see invocation)
# ^[[A^Cninja: build stopped: interrupted by user.
