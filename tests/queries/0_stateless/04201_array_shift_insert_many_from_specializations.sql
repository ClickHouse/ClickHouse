DROP TABLE IF EXISTS t_array_shift_imf;

-- Every row carries its OWN default, so a result that reused another row's default is visible.
CREATE TABLE t_array_shift_imf
(
    id UInt8,
    s Int8,
    fs_arr Array(FixedString(2)),  fs_def FixedString(2),
    dec_arr Array(Decimal128(3)),  dec_def Decimal128(3),
    n_arr Array(Nullable(Int64)),  n_def Nullable(Int64),
    t_arr Array(Tuple(UInt8, String)), t_def Tuple(UInt8, String),
    m_arr Array(Map(String, UInt8)),   m_def Map(String, UInt8)
) ENGINE = Memory;

INSERT INTO t_array_shift_imf VALUES
    (1, 1, ['aa','bb','cc'], 'xx', [1.5,2.5,3.5], 99.9, [1,NULL,3], 99, [(1,'a'),(2,'b'),(3,'c')], (7,'z'), [map('a',1),map('b',2),map('c',3)], map('z',7)),
    (2, 2, ['dd','ee','ff'], 'yy', [4.5,5.5,6.5], 88.8, [4,NULL,6], 88, [(4,'d'),(5,'e'),(6,'f')], (8,'y'), [map('d',4),map('e',5),map('f',6)], map('y',8)),
    (3, 3, ['gg','hh','ii'], 'zz', [7.5,8.5,9.5], 77.7, [7,NULL,9], 77, [(7,'g'),(8,'h'),(9,'i')], (9,'x'), [map('g',7),map('h',8),map('i',9)], map('x',9));

SELECT '== FixedString ==';
SELECT arrayShiftRight(fs_arr, s, fs_def) FROM t_array_shift_imf ORDER BY id;
SELECT arrayShiftLeft(fs_arr, s, fs_def) FROM t_array_shift_imf ORDER BY id;

SELECT '== Decimal128 ==';
SELECT arrayShiftRight(dec_arr, s, dec_def) FROM t_array_shift_imf ORDER BY id;

SELECT '== Nullable(Int64) ==';
SELECT arrayShiftRight(n_arr, s, n_def) FROM t_array_shift_imf ORDER BY id;
-- Omitting the default makes it NULL for a Nullable element type, which is the only shape
-- that fills the null map from the default rather than from the array.
SELECT arrayShiftRight(n_arr, s) FROM t_array_shift_imf ORDER BY id;

SELECT '== Tuple ==';
SELECT arrayShiftRight(t_arr, s, t_def) FROM t_array_shift_imf ORDER BY id;

SELECT '== Map ==';
SELECT arrayShiftRight(m_arr, s, m_def) FROM t_array_shift_imf ORDER BY id;

DROP TABLE t_array_shift_imf;
