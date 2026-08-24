SET send_logs_level = 'fatal';
SELECT arrayEnumerateUniq(anyHeavy([]), []);
SELECT arrayEnumerateDense([], [sequenceCount(NULL)]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT arrayEnumerateDense([STDDEV_SAMP(NULL, 910947.571364)], [NULL]);
