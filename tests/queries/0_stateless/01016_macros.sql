SELECT * FROM system.macros WHERE macro = 'test';
SELECT getMacro('test');
select isConstant(getMacro('test'));
