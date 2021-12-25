struct semid_ds {
	struct ipc_perm sem_perm;
	time_t sem_otime;
	time_t sem_ctime;
#if __BYTE_ORDER == __LITTLE_ENDIAN
	unsigned short sem_nsems;
	char __sem_nsems_pad[sizeof(long)-sizeof(short)];
#else
	char __sem_nsems_pad[sizeof(long)-sizeof(short)];
	unsigned short sem_nsems;
#endif
	long __unused3;
	long __unused4;
};
