select filesystemCapacity('default') >= filesystemAvailable('default') and filesystemAvailable('default') >= filesystemUnreserved('default');
select filesystemCapacity('__un_exists_disk'); -- { serverError UNKNOWN_DISK }
