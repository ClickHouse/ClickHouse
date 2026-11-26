-- Values near UInt64 max should not cause UB (UBSan: outside the range of representable values)
SELECT parseReadableSize('15 EiB');
SELECT parseReadableSize('18.4 EB');
SELECT parseReadableSize('16 EiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('18.5 EB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('18446744073709551616 B'); -- { serverError BAD_ARGUMENTS }
