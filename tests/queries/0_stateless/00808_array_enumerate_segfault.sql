SET send_logs_level = 'none';
SELECT arrayEnumerateUniq(anyHeavy([]), []);
SELECT arrayEnumerateDense([], [sequenceCount(NULL)]); -- { serverError 42 }
SELECT arrayEnumerateDense([STDDEV_SAMP(NULL, 910947.571364)], [NULL]);
