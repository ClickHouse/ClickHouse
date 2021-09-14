-- TODO: Tautological optimization breaks JIT expression compilation, because it can return constant result
-- for non constant columns. And then sample blocks from same ActionsDAGs can be mismatched.
-- This optimization cannot be performed on AST rewrite level, because we does not have information about types
-- and equals(tuple(NULL), tuple(NULL)) have same hash code, but should not be optimized.
-- Return this test after refactoring of InterpreterSelectQuery.

-- SELECT count() FROM system.numbers WHERE number != number;
-- SELECT count() FROM system.numbers WHERE number < number;
-- SELECT count() FROM system.numbers WHERE number > number;

-- SELECT count() FROM system.numbers WHERE NOT (number = number);
-- SELECT count() FROM system.numbers WHERE NOT (number <= number);
-- SELECT count() FROM system.numbers WHERE NOT (number >= number);

-- SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number));
-- SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number)) AND rand() > 10;

-- column_column_comparison.xml
-- <test>
--     <tags>
--         <tag>comparison</tag>
--     </tags>

--     <preconditions>
--         <table_exists>hits_100m_single</table_exists>
--     </preconditions>


--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE URL < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE URL < PageCharset]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE SearchPhrase < SearchPhrase SETTINGS max_threads = 2]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE SearchPhrase < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE SearchPhrase < PageCharset SETTINGS max_threads = 2]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(SearchPhrase) AND SearchPhrase < SearchPhrase SETTINGS max_threads = 2]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(SearchPhrase) AND SearchPhrase < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(SearchPhrase) AND SearchPhrase < PageCharset SETTINGS max_threads = 2]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE MobilePhoneModel < MobilePhoneModel SETTINGS max_threads = 1]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE MobilePhoneModel < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE MobilePhoneModel < PageCharset SETTINGS max_threads = 2]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(MobilePhoneModel) AND MobilePhoneModel < MobilePhoneModel SETTINGS max_threads = 1]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(MobilePhoneModel) AND MobilePhoneModel < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE notEmpty(MobilePhoneModel) AND MobilePhoneModel < PageCharset SETTINGS max_threads = 2]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE PageCharset < PageCharset SETTINGS max_threads = 2]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE PageCharset < URL]]></query>
--     <query short="1"><![CDATA[SELECT count() FROM hits_100m_single WHERE Title < Title]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE Title < URL]]></query>
--     <query><![CDATA[SELECT count() FROM hits_100m_single WHERE Title < PageCharset]]></query>

-- </test>
