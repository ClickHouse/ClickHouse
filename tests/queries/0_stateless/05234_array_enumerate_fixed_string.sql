SELECT arrayEnumerateUniq(CAST(['a', 'b', 'a', 'c', 'a'] AS Array(FixedString(16))));
SELECT arrayEnumerateDense(CAST(['a', 'b', 'a', 'c', 'a'] AS Array(FixedString(16))));
SELECT arrayEnumerateUniq(CAST(['alpha', 'beta', 'alpha', 'gamma', 'beta'] AS Array(FixedString(32))));
SELECT arrayEnumerateDense(CAST(['alpha', 'beta', 'alpha', 'gamma', 'beta'] AS Array(FixedString(32))));
