
set (ENABLE_CEPH_DEFAULT 0)
# darwin, riscv, musl, loongson: missing -lresolv
if (CMAKE_SYSTEM_NAME MATCHES "Linux" AND NOT USE_MUSL AND (ARCH_AMD64 OR ARCH_AARCH64 OR ARCH_S390X))
    set (ENABLE_CEPH_DEFAULT ${ENABLE_LIBRARIES})
else()
    message (WARNING "Ceph is only available on linux for amd64 and aarch64")
endif()

OPTION(ENABLE_CEPH "Enable Ceph" ${ENABLE_CEPH_DEFAULT})

if (NOT ENABLE_CEPH)
    message(STATUS "Not using ceph")
    return()
endif()

set (Ceph_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/ceph")
set (Ceph_CMAKE_DIR "${ClickHouse_SOURCE_DIR}/contrib/ceph-cmake")
set (CMAKE_CXX_STANDARD 20)

### common ceph infrastructure - but we pregenrated the files so don't need this
### keep as reference

# the src/ceph_release file is 3 lines,
#   <release number, e.g. '12' for luminous>
#   <release name, e.g. 'luminous'>
#   <release type: 'dev' for x.0.z, 'rc' or x.1.z, or 'stable' or x.2.z>
# note that the release name is semi-redundant and must match CEPH_RELEASE_*
# definitions in include/rados.h and common/ceph_strings.c.

# file(STRINGS ${Ceph_LIBRARY_DIR}/src/ceph_release CEPH_RELEASE_FILE)
# list(GET CEPH_RELEASE_FILE 0 CEPH_RELEASE)
# list(GET CEPH_RELEASE_FILE 1 CEPH_RELEASE_NAME)
# list(GET CEPH_RELEASE_FILE 2 CEPH_RELEASE_TYPE)

# configure_file(
#   ${Ceph_LIBRARY_DIR}/src/include/config-h.in.cmake
#   ${Ceph_BUILD_DIR}/acconfig.h
# )

# configure_file(
#   ${Ceph_LIBRARY_DIR}/src/ceph_ver.h.in.cmake
#   ${Ceph_BUILD_DIR}/ceph_ver.h
#   @ONLY)


# Below code is for generating options from yaml files. In ClickHouse build we
# pre-generated these files in ${Ceph_CMAKE_DIR}/Options/ and ${Ceph_CMAKE_DIR}
# Even only librados required all options to build.
# Must have python3 and pyyaml installed

# set(OPTIONS_DIR "${Ceph_LIBRARY_DIR}/src/common/options")
# function(add_options name)
#   set(yaml_in_file ${OPTIONS_DIR}/${name}.yaml.in)
#   set(yaml_file ${Ceph_CMAKE_DIR}/Options/${name}.yaml)
#   configure_file("${yaml_in_file}"
#     "${yaml_file}" @ONLY)
#   set(cc_file "${Ceph_CMAKE_DIR}/${name}_options.cc")
#   set(h_file "${Ceph_CMAKE_DIR}/Options/${name}_legacy_options.h")
#   execute_process(
#     COMMAND python3 ${OPTIONS_DIR}/y2c.py
#       --input ${yaml_file}
#       --output ${cc_file}
#       --legacy ${h_file}
#       --name ${name}
#       COMMAND_ECHO STDOUT)
# endfunction()

# add_options(global)
# add_options(cephfs-mirror)
# add_options(crimson)
# add_options(mgr)
# add_options(mds)
# add_options(mds-client)
# add_options(mon)
# add_options(osd)
# add_options(rbd)
# add_options(rbd-mirror)
# add_options(immutable-object-cache)
# add_options(ceph-exporter)
# add_options(rgw)

set(SRCS

    ### build options
    "${Ceph_CMAKE_DIR}/Options/rgw_options.cc"
    "${Ceph_CMAKE_DIR}/Options/mgr_options.cc"
    "${Ceph_CMAKE_DIR}/Options/rbd-mirror_options.cc"
    "${Ceph_CMAKE_DIR}/Options/mds-client_options.cc"
    "${Ceph_CMAKE_DIR}/Options/osd_options.cc"
    "${Ceph_CMAKE_DIR}/Options/global_options.cc"
    "${Ceph_CMAKE_DIR}/Options/rbd_options.cc"
    "${Ceph_CMAKE_DIR}/Options/ceph-exporter_options.cc"
    "${Ceph_CMAKE_DIR}/Options/immutable-object-cache_options.cc"
    "${Ceph_CMAKE_DIR}/Options/cephfs-mirror_options.cc"
    "${Ceph_CMAKE_DIR}/Options/crimson_options.cc"
    "${Ceph_CMAKE_DIR}/Options/mds_options.cc"
    "${Ceph_CMAKE_DIR}/Options/mon_options.cc"
    "${Ceph_LIBRARY_DIR}/src/common/options/build_options.cc"

    ### auth
    "${Ceph_LIBRARY_DIR}/src/auth/RotatingKeyRing.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/Crypto.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/cephx/CephxSessionHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/cephx/CephxAuthorizeHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/cephx/CephxProtocol.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/cephx/CephxClientHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/AuthClientHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/AuthRegistry.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/AuthSessionHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/KeyRing.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/none/AuthNoneAuthorizeHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/auth/AuthMethodList.cc"

    ### ceph-common
    "${Ceph_LIBRARY_DIR}/src/common/utf8.c"
    "${Ceph_LIBRARY_DIR}/src/common/buffer.cc"
    "${Ceph_LIBRARY_DIR}/src/common/hobject.cc"
    "${Ceph_LIBRARY_DIR}/src/common/escape.cc"
    "${Ceph_LIBRARY_DIR}/src/common/OutputDataSocket.cc"
    "${Ceph_LIBRARY_DIR}/src/common/config.cc"
    "${Ceph_LIBRARY_DIR}/src/common/pick_address.cc"
    "${Ceph_LIBRARY_DIR}/src/common/hostname.cc"
    "${Ceph_LIBRARY_DIR}/src/common/url_escape.cc"
    "${Ceph_LIBRARY_DIR}/src/common/hex.cc"
    "${Ceph_LIBRARY_DIR}/src/common/HTMLFormatter.cc"
    "${Ceph_LIBRARY_DIR}/src/common/bit_str.cc"
    "${Ceph_LIBRARY_DIR}/src/common/DecayCounter.cc"
    "${Ceph_LIBRARY_DIR}/src/common/entity_name.cc"
    "${Ceph_LIBRARY_DIR}/src/common/common_init.cc"
    "${Ceph_LIBRARY_DIR}/src/common/types.cc"
    "${Ceph_LIBRARY_DIR}/src/common/fd.cc"
    "${Ceph_LIBRARY_DIR}/src/common/TracepointProvider.cc"
    "${Ceph_LIBRARY_DIR}/src/common/LogEntry.cc"
    "${Ceph_LIBRARY_DIR}/src/common/perf_counters_collection.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Finisher.cc"
    "${Ceph_LIBRARY_DIR}/src/common/strtol.cc"
    "${Ceph_LIBRARY_DIR}/src/common/reverse.c"
    "${Ceph_LIBRARY_DIR}/src/common/Formatter.cc"
    "${Ceph_LIBRARY_DIR}/src/common/errno.cc"
    "${Ceph_LIBRARY_DIR}/src/common/utf8.c"
    "${Ceph_LIBRARY_DIR}/src/common/Throttle.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Readahead.cc"
    "${Ceph_LIBRARY_DIR}/src/common/CDC.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Graylog.cc"
    "${Ceph_LIBRARY_DIR}/src/common/environment.cc"
    "${Ceph_LIBRARY_DIR}/src/common/FixedCDC.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_time.cc"
    "${Ceph_LIBRARY_DIR}/src/common/admin_socket.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_json.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_hash.cc"
    "${Ceph_LIBRARY_DIR}/src/common/FastCDC.cc"
    "${Ceph_LIBRARY_DIR}/src/common/fs_types.cc"
    "${Ceph_LIBRARY_DIR}/src/common/perf_counters_key.cc"
    "${Ceph_LIBRARY_DIR}/src/common/snap_types.cc"
    "${Ceph_LIBRARY_DIR}/src/common/linux_version.c"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_crypto.cc"
    "${Ceph_LIBRARY_DIR}/src/common/AsyncOpTracker.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ostream_temp.cc"
    "${Ceph_LIBRARY_DIR}/src/common/cmdparse.cc"
    "${Ceph_LIBRARY_DIR}/src/common/run_cmd.cc"
    "${Ceph_LIBRARY_DIR}/src/common/perf_counters.cc"
    "${Ceph_LIBRARY_DIR}/src/common/BackTrace.cc"
    "${Ceph_LIBRARY_DIR}/src/common/LogClient.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_frag.cc"
    "${Ceph_LIBRARY_DIR}/src/common/dout.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_argparse.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_releases.cc"
    "${Ceph_LIBRARY_DIR}/src/common/HeartbeatMap.cc"
    "${Ceph_LIBRARY_DIR}/src/common/bloom_filter.cc"
    "${Ceph_LIBRARY_DIR}/src/common/assert.cc"
    "${Ceph_LIBRARY_DIR}/src/common/RefCountedObj.cc"
    "${Ceph_LIBRARY_DIR}/src/common/random_string.cc"
    "${Ceph_LIBRARY_DIR}/src/common/str_list.cc"
    "${Ceph_LIBRARY_DIR}/src/common/perf_counters_cache.cc"
    "${Ceph_LIBRARY_DIR}/src/common/code_environment.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Cycles.cc"
    "${Ceph_LIBRARY_DIR}/src/common/WorkQueue.cc"
    "${Ceph_LIBRARY_DIR}/src/common/compat.cc"
    "${Ceph_LIBRARY_DIR}/src/common/numa.cc"
    "${Ceph_LIBRARY_DIR}/src/common/mempool.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_fs.cc"
    "${Ceph_LIBRARY_DIR}/src/common/iso_8601.cc"
    "${Ceph_LIBRARY_DIR}/src/common/SubProcess.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Thread.cc"
    "${Ceph_LIBRARY_DIR}/src/common/options.cc"
    "${Ceph_LIBRARY_DIR}/src/common/scrub_types.cc"
    "${Ceph_LIBRARY_DIR}/src/common/mime.c"
    "${Ceph_LIBRARY_DIR}/src/common/histogram.cc"
    "${Ceph_LIBRARY_DIR}/src/common/str_map.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_context.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ipaddr.cc"
    "${Ceph_LIBRARY_DIR}/src/common/dns_resolve.cc"
    "${Ceph_LIBRARY_DIR}/src/common/TrackedOp.cc"
    "${Ceph_LIBRARY_DIR}/src/common/page.cc"
    "${Ceph_LIBRARY_DIR}/src/common/PluginRegistry.cc"
    "${Ceph_LIBRARY_DIR}/src/common/SloppyCRCMap.cc"
    "${Ceph_LIBRARY_DIR}/src/common/signal.cc"
    "${Ceph_LIBRARY_DIR}/src/common/pretty_binary.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ConfUtils.cc"
    "${Ceph_LIBRARY_DIR}/src/common/version.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_strings.cc"
    "${Ceph_LIBRARY_DIR}/src/common/Timer.cc"
    "${Ceph_LIBRARY_DIR}/src/common/admin_socket_client.cc"
    "${Ceph_LIBRARY_DIR}/src/common/openssl_opts_handler.cc"
    "${Ceph_LIBRARY_DIR}/src/common/perf_histogram.cc"
    "${Ceph_LIBRARY_DIR}/src/common/config_values.cc"
    "${Ceph_LIBRARY_DIR}/src/common/tracer.cc"
    "${Ceph_LIBRARY_DIR}/src/common/error_code.cc"
    "${Ceph_LIBRARY_DIR}/src/common/safe_io.c"
    "${Ceph_LIBRARY_DIR}/src/common/armor.c"
    "${Ceph_LIBRARY_DIR}/src/common/addr_parsing.c"
    "${Ceph_LIBRARY_DIR}/src/common/TextTable.cc"
    "${Ceph_LIBRARY_DIR}/src/common/ceph_regex.cc"
    "${Ceph_LIBRARY_DIR}/src/common/sctp_crc32.c"
    "${Ceph_LIBRARY_DIR}/src/common/crc32c.cc"
    "${Ceph_LIBRARY_DIR}/src/common/crc32c_intel_baseline.c"
    "${Ceph_LIBRARY_DIR}/src/common/crc32c_intel_fast.c"
    "${Ceph_LIBRARY_DIR}/src/erasure-code/ErasureCodePlugin.cc"
    "${Ceph_LIBRARY_DIR}/src/compressor/Compressor.cc"
    "${Ceph_LIBRARY_DIR}/src/arch/intel.c"
    "${Ceph_LIBRARY_DIR}/src/arch/arm.c"
    "${Ceph_LIBRARY_DIR}/src/arch/ppc.c"
    "${Ceph_LIBRARY_DIR}/src/arch/probe.cc"
    "${Ceph_LIBRARY_DIR}/src/ceph_ver.c"
    "${Ceph_LIBRARY_DIR}/src/log/Log.cc"
    "${Ceph_LIBRARY_DIR}/src/global/global_context.cc"

    ### json_spirit
    "${Ceph_LIBRARY_DIR}/src/json_spirit/json_spirit_reader.cpp"
    "${Ceph_LIBRARY_DIR}/src/json_spirit/json_spirit_writer.cpp"
    "${Ceph_LIBRARY_DIR}/src/json_spirit/json_spirit_value.cpp"

    ### librbd
    "${Ceph_LIBRARY_DIR}/src/librbd/io/IoOperations.cc"
    "${Ceph_LIBRARY_DIR}/src/librbd/Features.cc"

    ### mgr
    "${Ceph_LIBRARY_DIR}/src/mgr/ServiceMap.cc"
    "${Ceph_LIBRARY_DIR}/src/mgr/MgrClient.cc"

    ### msg
    "${Ceph_LIBRARY_DIR}/src/msg/Connection.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/compressor_registry.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/Message.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/ProtocolV2.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/compression_onwire.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/crypto_onwire.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/ProtocolV1.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/Stack.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/frames_v2.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/AsyncConnection.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/EventEpoll.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/PosixStack.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/Protocol.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/net_handler.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/Event.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/AsyncMessenger.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/async/EventSelect.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/Messenger.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/DispatchQueue.cc"
    "${Ceph_LIBRARY_DIR}/src/msg/msg_types.cc"

    ### mds
    "${Ceph_LIBRARY_DIR}/src/mds/flock.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/MDSMap.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/inode_backtrace.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/cephfs_features.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/MDSAuthCaps.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/FSMap.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/mdstypes.cc"
    "${Ceph_LIBRARY_DIR}/src/mds/FSMapUser.cc"

    ### osd
    "${Ceph_LIBRARY_DIR}/src/osd/OSDMapMapping.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/PGPeeringEvent.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/error_code.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/osd_op_util.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/OpRequest.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/OSDMap.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/osd_types.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/ClassHandler.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/ECMsgTypes.cc"
    "${Ceph_LIBRARY_DIR}/src/osd/HitSet.cc"

    ### osdc
    "${Ceph_LIBRARY_DIR}/src/osdc/Filer.cc"
    "${Ceph_LIBRARY_DIR}/src/osdc/ObjectCacher.cc"
    "${Ceph_LIBRARY_DIR}/src/osdc/Objecter.cc"
    "${Ceph_LIBRARY_DIR}/src/osdc/error_code.cc"
    "${Ceph_LIBRARY_DIR}/src/osdc/Striper.cc"

    ### mon
    "${Ceph_LIBRARY_DIR}/src/mon/MonCap.cc"
    "${Ceph_LIBRARY_DIR}/src/mon/PGMap.cc"
    "${Ceph_LIBRARY_DIR}/src/mon/error_code.cc"
    "${Ceph_LIBRARY_DIR}/src/mon/MonMap.cc"
    "${Ceph_LIBRARY_DIR}/src/mon/MonSub.cc"
    "${Ceph_LIBRARY_DIR}/src/mon/MonClient.cc"

    ### crush
    "${Ceph_LIBRARY_DIR}/src/crush/mapper.c"
    "${Ceph_LIBRARY_DIR}/src/crush/CrushCompiler.cc"
    "${Ceph_LIBRARY_DIR}/src/crush/crush.c"
    "${Ceph_LIBRARY_DIR}/src/crush/builder.c"
    "${Ceph_LIBRARY_DIR}/src/crush/hash.c"
    "${Ceph_LIBRARY_DIR}/src/crush/CrushLocation.cc"
    "${Ceph_LIBRARY_DIR}/src/crush/CrushWrapper.cc"
    "${Ceph_LIBRARY_DIR}/src/crush/CrushTester.cc"

    ### cls_lock_client
    "${Ceph_LIBRARY_DIR}/src/cls/lock/cls_lock_ops.cc"
    "${Ceph_LIBRARY_DIR}/src/cls/lock/cls_lock_client.cc"
    "${Ceph_LIBRARY_DIR}/src/cls/lock/cls_lock_types.cc"

    ### librados_impl
    "${Ceph_LIBRARY_DIR}/src/librados/IoCtxImpl.cc"
    "${Ceph_LIBRARY_DIR}/src/librados/RadosXattrIter.cc"
    "${Ceph_LIBRARY_DIR}/src/librados/RadosClient.cc"
    "${Ceph_LIBRARY_DIR}/src/librados/librados_util.cc"
    "${Ceph_LIBRARY_DIR}/src/librados/librados_tp.cc"

    ### librados
    "${Ceph_LIBRARY_DIR}/src/librados/librados_c.cc"
    "${Ceph_LIBRARY_DIR}/src/librados/librados_cxx.cc"
)

add_library(_librados ${SRCS})
add_library(ch_contrib::ceph ALIAS _librados)

target_include_directories(_librados PRIVATE "${Ceph_LIBRARY_DIR}/src/" "${Ceph_CMAKE_DIR}/Options")
target_include_directories(_librados SYSTEM BEFORE INTERFACE "${Ceph_LIBRARY_DIR}/src/include/rados")

### librados required definitions
target_compile_definitions(_librados PRIVATE HAVE_CONFIG_H __CEPH__ CEPH_USE_RE2 LIBCXX_NO_STRING_SPACESHIP CMAKE_SHARED_LIBRARY_SUFFIX=".so")

target_link_libraries(_librados
    PRIVATE
        boost::headers_only
        boost::iostreams
        boost::thread
        ch_contrib::fmt
        ### re2 and its dependency (absl)
        ch_contrib::re2
        absl::base
        absl::core_headers
        absl::fixed_array
        absl::flat_hash_map
        absl::flat_hash_set
        absl::inlined_vector
        absl::strings
        absl::str_format
        absl::synchronization
        absl::optional
        absl::span
        OpenSSL::Crypto
        resolv
)

### compile a test program
add_executable(hello_world_rados "${Ceph_LIBRARY_DIR}/examples/librados/hello_world.cc")
target_include_directories(hello_world_rados PRIVATE "${Ceph_LIBRARY_DIR}/src/include")
target_link_libraries(hello_world_rados PUBLIC ch_contrib::ceph)
