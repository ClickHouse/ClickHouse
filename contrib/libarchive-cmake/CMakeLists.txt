set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libarchive")

set(SRCS
    "${LIBRARY_DIR}/libarchive/archive_acl.c"
#    "${LIBRARY_DIR}/libarchive/archive_blake2sp_ref.c"
#    "${LIBRARY_DIR}/libarchive/archive_blake2s_ref.c"
    "${LIBRARY_DIR}/libarchive/archive_check_magic.c"
    "${LIBRARY_DIR}/libarchive/archive_cmdline.c"
    "${LIBRARY_DIR}/libarchive/archive_cryptor.c"
    "${LIBRARY_DIR}/libarchive/archive_digest.c"
    "${LIBRARY_DIR}/libarchive/archive_disk_acl_darwin.c"
    "${LIBRARY_DIR}/libarchive/archive_disk_acl_freebsd.c"
    "${LIBRARY_DIR}/libarchive/archive_disk_acl_linux.c"
    "${LIBRARY_DIR}/libarchive/archive_disk_acl_sunos.c"
    "${LIBRARY_DIR}/libarchive/archive_entry.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_copy_bhfi.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_copy_stat.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_link_resolver.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_sparse.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_stat.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_strmode.c"
    "${LIBRARY_DIR}/libarchive/archive_entry_xattr.c"
    "${LIBRARY_DIR}/libarchive/archive_getdate.c"
    "${LIBRARY_DIR}/libarchive/archive_hmac.c"
    "${LIBRARY_DIR}/libarchive/archive_match.c"
    "${LIBRARY_DIR}/libarchive/archive_options.c"
    "${LIBRARY_DIR}/libarchive/archive_pack_dev.c"
    "${LIBRARY_DIR}/libarchive/archive_pathmatch.c"
    "${LIBRARY_DIR}/libarchive/archive_ppmd7.c"
    "${LIBRARY_DIR}/libarchive/archive_ppmd8.c"
    "${LIBRARY_DIR}/libarchive/archive_random.c"
    "${LIBRARY_DIR}/libarchive/archive_rb.c"
    "${LIBRARY_DIR}/libarchive/archive_read_add_passphrase.c"
    "${LIBRARY_DIR}/libarchive/archive_read_append_filter.c"
    "${LIBRARY_DIR}/libarchive/archive_read.c"
    "${LIBRARY_DIR}/libarchive/archive_read_data_into_fd.c"
    "${LIBRARY_DIR}/libarchive/archive_read_disk_entry_from_file.c"
    "${LIBRARY_DIR}/libarchive/archive_read_disk_posix.c"
    "${LIBRARY_DIR}/libarchive/archive_read_disk_set_standard_lookup.c"
#    "${LIBRARY_DIR}/libarchive/archive_read_disk_windows.c"
    "${LIBRARY_DIR}/libarchive/archive_read_extract2.c"
    "${LIBRARY_DIR}/libarchive/archive_read_extract.c"
    "${LIBRARY_DIR}/libarchive/archive_read_open_fd.c"
    "${LIBRARY_DIR}/libarchive/archive_read_open_file.c"
    "${LIBRARY_DIR}/libarchive/archive_read_open_filename.c"
    "${LIBRARY_DIR}/libarchive/archive_read_open_memory.c"
    "${LIBRARY_DIR}/libarchive/archive_read_set_format.c"
    "${LIBRARY_DIR}/libarchive/archive_read_set_options.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_all.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_by_code.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_bzip2.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_compress.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_grzip.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_gzip.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_lrzip.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_lz4.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_lzop.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_none.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_program.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_rpm.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_uu.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_xz.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_filter_zstd.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_7zip.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_all.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_ar.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_by_code.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_cab.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_cpio.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_empty.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_iso9660.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_lha.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_mtree.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_rar5.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_rar.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_raw.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_tar.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_warc.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_xar.c"
    "${LIBRARY_DIR}/libarchive/archive_read_support_format_zip.c"
    "${LIBRARY_DIR}/libarchive/archive_string.c"
    "${LIBRARY_DIR}/libarchive/archive_string_sprintf.c"
    "${LIBRARY_DIR}/libarchive/archive_util.c"
    "${LIBRARY_DIR}/libarchive/archive_version_details.c"
    "${LIBRARY_DIR}/libarchive/archive_virtual.c"
    "${LIBRARY_DIR}/libarchive/archive_windows.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_b64encode.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_by_name.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_bzip2.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_compress.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_grzip.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_gzip.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_lrzip.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_lz4.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_lzop.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_none.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_program.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_uuencode.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_xz.c"
    "${LIBRARY_DIR}/libarchive/archive_write_add_filter_zstd.c"
    "${LIBRARY_DIR}/libarchive/archive_write.c"
    "${LIBRARY_DIR}/libarchive/archive_write_disk_posix.c"
    "${LIBRARY_DIR}/libarchive/archive_write_disk_set_standard_lookup.c"
    "${LIBRARY_DIR}/libarchive/archive_write_disk_windows.c"
    "${LIBRARY_DIR}/libarchive/archive_write_open_fd.c"
    "${LIBRARY_DIR}/libarchive/archive_write_open_file.c"
    "${LIBRARY_DIR}/libarchive/archive_write_open_filename.c"
    "${LIBRARY_DIR}/libarchive/archive_write_open_memory.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_7zip.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_ar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_by_name.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_cpio_binary.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_cpio.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_cpio_newc.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_cpio_odc.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_filter_by_ext.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_gnutar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_iso9660.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_mtree.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_pax.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_raw.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_shar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_ustar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_v7tar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_warc.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_xar.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_format_zip.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_options.c"
    "${LIBRARY_DIR}/libarchive/archive_write_set_passphrase.c"
    "${LIBRARY_DIR}/libarchive/filter_fork_posix.c"
    "${LIBRARY_DIR}/libarchive/filter_fork_windows.c"
    "${LIBRARY_DIR}/libarchive/xxhash.c"
)

add_library(_libarchive ${SRCS})
target_include_directories(_libarchive PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    "${LIBRARY_DIR}/libarchive"
)

target_compile_definitions(_libarchive PUBLIC
    HAVE_CONFIG_H
)

target_compile_options(_libarchive PRIVATE "-Wno-reserved-macro-identifier")

if (TARGET ch_contrib::xz)
    target_compile_definitions(_libarchive PUBLIC HAVE_LZMA_H=1 HAVE_LIBLZMA=1)
    target_link_libraries(_libarchive PRIVATE ch_contrib::xz)
endif()

if (TARGET ch_contrib::zlib)
    target_compile_definitions(_libarchive PUBLIC HAVE_ZLIB_H=1)
    target_link_libraries(_libarchive PRIVATE ch_contrib::zlib)
endif()

if (TARGET ch_contrib::zstd)
    target_compile_definitions(_libarchive PUBLIC HAVE_ZSTD_H=1 HAVE_LIBZSTD=1 HAVE_ZSTD_compressStream=1)
    target_link_libraries(_libarchive PRIVATE ch_contrib::zstd)
endif()

if (TARGET ch_contrib::bzip2)
    target_compile_definitions(_libarchive PUBLIC HAVE_BZLIB_H=1)
    target_link_libraries(_libarchive PRIVATE ch_contrib::bzip2)
endif()

if (OS_LINUX)
    target_compile_definitions(
        _libarchive PUBLIC
            MAJOR_IN_SYSMACROS=1
            HAVE_LINUX_FS_H=1
            HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC=1
            HAVE_LINUX_TYPES_H=1
            HAVE_SYS_STATFS_H=1
            HAVE_FUTIMESAT=1
            HAVE_ICONV=1
    )
endif()

add_library(ch_contrib::libarchive ALIAS _libarchive)
