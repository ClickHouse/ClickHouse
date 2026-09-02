-- Tags: no-fasttest

select filesystemCapacity('s3_disk') >= filesystemAvailable('s3_disk') and filesystemAvailable('s3_disk') >= filesystemUnreserved('s3_disk');
select filesystemCapacity('default') >= filesystemAvailable('default') and filesystemAvailable('default') >= 0 and filesystemUnreserved('default') >= 0;

select filesystemCapacity('__un_exists_disk'); -- { serverError UNKNOWN_DISK }
