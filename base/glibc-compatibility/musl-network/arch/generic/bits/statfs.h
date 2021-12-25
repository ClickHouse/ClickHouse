struct statfs {
	unsigned long f_type, f_bsize;
	fsblkcnt_t f_blocks, f_bfree, f_bavail;
	fsfilcnt_t f_files, f_ffree;
	fsid_t f_fsid;
	unsigned long f_namelen, f_frsize, f_flags, f_spare[4];
};
