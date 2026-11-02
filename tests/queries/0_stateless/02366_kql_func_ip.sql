set allow_experimental_kusto_dialect=1;
set dialect='kusto';
print '-- ipv4_is_private(\'127.0.0.1\')';
print ipv4_is_private('127.0.0.1');
print '-- ipv4_is_private(\'10.1.2.3\')';
print ipv4_is_private('10.1.2.3');
print '-- ipv4_is_private(\'192.168.1.1/24\')';
print ipv4_is_private('192.168.1.1/24');
print 'ipv4_is_private(strcat(\'192.\',\'168.\',\'1.\',\'1\',\'/24\'))';
print ipv4_is_private(strcat('192.','168.','1.','1','/24'));
print '-- ipv4_is_private(\'abc\')';
print ipv4_is_private('abc'); -- == null

print '-- ipv4_netmask_suffix(\'192.168.1.1/24\')';
print ipv4_netmask_suffix('192.168.1.1/24'); -- == 24
print '-- ipv4_netmask_suffix(\'192.168.1.1\')';
print ipv4_netmask_suffix('192.168.1.1'); -- == 32
print '-- ipv4_netmask_suffix(\'127.0.0.1/16\')';
print ipv4_netmask_suffix('127.0.0.1/16'); -- == 16
print '-- ipv4_netmask_suffix(\'abc\')';
print ipv4_netmask_suffix('abc'); -- == null
print 'ipv4_netmask_suffix(strcat(\'127.\', \'0.\', \'0.1/16\'))';
print ipv4_netmask_suffix(strcat('127.', '0.', '0.1/16')); -- == 16

print '-- ipv4_is_in_range(\'127.0.0.1\', \'127.0.0.1\')';
print ipv4_is_in_range('127.0.0.1', '127.0.0.1'); -- == true
print '-- ipv4_is_in_range(\'192.168.1.6\', \'192.168.1.1/24\')';
print ipv4_is_in_range('192.168.1.6', '192.168.1.1/24'); -- == true
print '-- ipv4_is_in_range(\'192.168.1.1\', \'192.168.2.1/24\')';
print ipv4_is_in_range('192.168.1.1', '192.168.2.1/24'); -- == false
print '-- ipv4_is_in_range(strcat(\'192.\',\'168.\', \'1.1\'), \'192.168.2.1/24\')';
print ipv4_is_in_range(strcat('192.','168.', '1.1'), '192.168.2.1/24'); -- == false
print '-- ipv4_is_in_range(\'abc\', \'127.0.0.1\')'; -- == null
print ipv4_is_in_range('abc', '127.0.0.1');

print '-- parse_ipv6(127.0.0.1)';
print parse_ipv6('127.0.0.1');
print '-- parse_ipv6(fe80::85d:e82c:9446:7994)';
print parse_ipv6('fe80::85d:e82c:9446:7994');
print '-- parse_ipv4(\'127.0.0.1\')';
print parse_ipv4('127.0.0.1');
print '-- parse_ipv4(\'192.1.168.1\') < parse_ipv4(\'192.1.168.2\')';
print parse_ipv4('192.1.168.1') < parse_ipv4('192.1.168.2');
print '-- parse_ipv4(arrayStringConcat([\'127\', \'0\', \'0\', \'1\'], \'.\'))';
print parse_ipv4(arrayStringConcat(['127', '0', '0', '1'], '.')); -- { clientError UNKNOWN_FUNCTION }

print '-- parse_ipv4_mask(\'127.0.0.1\', 24) == 2130706432';
print parse_ipv4_mask('127.0.0.1', 24);
print '-- parse_ipv4_mask(\'abc\', 31)';
print parse_ipv4_mask('abc', 31)
print '-- parse_ipv4_mask(\'192.1.168.2\', 1000)';
print parse_ipv4_mask('192.1.168.2', 1000);
print '-- parse_ipv4_mask(\'192.1.168.2\', 31) == parse_ipv4_mask(\'192.1.168.3\', 31)';
--print parse_ipv4_mask('192.1.168.2', 31) == parse_ipv4_mask('192.1.168.3', 31); // this qual failed in analyzer 3221334018
print parse_ipv4_mask('192.1.168.2', 31);
print parse_ipv4_mask('192.1.168.3', 31);
print '-- ipv4_is_match(\'127.0.0.1\', \'127.0.0.1\')';
print ipv4_is_match('127.0.0.1', '127.0.0.1');
print '-- ipv4_is_match(\'192.168.1.1\', \'192.168.1.255\')';
print ipv4_is_match('192.168.1.1', '192.168.1.255');
print '-- ipv4_is_match(\'192.168.1.1/24\', \'192.168.1.255/24\')';
print ipv4_is_match('192.168.1.1/24', '192.168.1.255/24');
print '-- ipv4_is_match(\'192.168.1.1\', \'192.168.1.255\', 24)';
print ipv4_is_match('192.168.1.1', '192.168.1.255', 24);
print '-- ipv4_is_match(\'abc\', \'def\', 24)';
print ipv4_is_match('abc', 'dev', 24);
print '-- ipv4_compare()';
print ipv4_compare('127.0.0.1', '127.0.0.1');
print ipv4_compare('192.168.1.1', '192.168.1.255');
print ipv4_compare('192.168.1.255', '192.168.1.1');
print ipv4_compare('192.168.1.1/24', '192.168.1.255/24');
print ipv4_compare('192.168.1.1', '192.168.1.255', 24);
print ipv4_compare('192.168.1.1/24', '192.168.1.255');
print ipv4_compare('192.168.1.1', '192.168.1.255/24');
print ipv4_compare('192.168.1.1/30', '192.168.1.255/24');
print ipv4_compare('192.168.1.1', '192.168.1.0', 31);
print ipv4_compare('192.168.1.1/24', '192.168.1.255', 31);
print ipv4_compare('192.168.1.1', '192.168.1.255', 24);
print '-- format_ipv4()';
print format_ipv4('192.168.1.255', 24);
print format_ipv4('192.168.1.1', 32);
print format_ipv4('192.168.1.1/24', 32);
print format_ipv4(3232236031, 24);
print format_ipv4('192.168.1.1/24', -1) == '';
print format_ipv4('abc', 24) == '';
print format_ipv4(strcat('127.0', '.0.', '1', '/32'), 12 + 12);
print '-- format_ipv4_mask()';
print format_ipv4_mask('192.168.1.255', 24);
print format_ipv4_mask(3232236031, 24);
print format_ipv4_mask('192.168.1.1', 24);
print format_ipv4_mask('192.168.1.1', 32);
print format_ipv4_mask('192.168.1.1/24', 32);
print format_ipv4_mask('192.168.1.1/24', -1) == '';
print format_ipv4_mask('abc', 24) == '';
print format_ipv4_mask(strcat('127.0', '.0.', '1', '/32'), 12 + 12);
print '-- parse_ipv6_mask()';
print parse_ipv6_mask("127.0.0.1", 24);
print parse_ipv6_mask("fe80::85d:e82c:9446:7994", 120);
print parse_ipv6_mask("192.168.255.255", 120);
print parse_ipv6_mask("192.168.255.255/24", 124);
print parse_ipv6_mask("255.255.255.255", 128);
print parse_ipv6_mask("fe80::85d:e82c:9446:7994", 128);
print parse_ipv6_mask("fe80::85d:e82c:9446:7994/120", 124);
print parse_ipv6_mask("::192.168.255.255", 128);
print parse_ipv6_mask("::192.168.255.255/24", 128);
print '-- ipv6_is_match()';
print ipv6_is_match('::ffff:7f00:1', '127.0.0.1') == true;
print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995') == false;
print ipv6_is_match('192.168.1.1/24', '192.168.1.255/24') == true;
print ipv6_is_match('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7995/127') == true;
print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995', 127) == true;
print ipv6_is_match('192.168.1.1',    '192.168.1.1'); --       // Equal IPs
print ipv6_is_match('192.168.1.1/24', '192.168.1.255'); --     // 24 bit IP4-prefix is used for comparison
print ipv6_is_match('192.168.1.1',    '192.168.1.255/24'); --  // 24 bit IP4-prefix is used for comparison
print ipv6_is_match('192.168.1.1/30', '192.168.1.255/24'); --  // 24 bit IP4-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7994'); --         // Equal IPs
print ipv6_is_match('fe80::85d:e82c:9446:7994/120', 'fe80::85d:e82c:9446:7998'); --     // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7998/120'); --     // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994/120', 'fe80::85d:e82c:9446:7998/120'); -- // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('192.168.1.1',      '::ffff:c0a8:0101'); -- // Equal IPs
print ipv6_is_match('192.168.1.1/24',   '::ffff:c0a8:01ff'); -- // 24 bit IP-prefix is used for comparison
print ipv6_is_match('::ffff:c0a8:0101', '192.168.1.255/24'); -- // 24 bit IP-prefix is used for comparison
print ipv6_is_match('::192.168.1.1/30', '192.168.1.255/24'); -- // 24 bit IP-prefix is used for comparison
print ipv6_is_match('192.168.1.1',    '192.168.1.0',   31); -- // 31 bit IP4-prefix is used for comparison
print ipv6_is_match('192.168.1.1/24', '192.168.1.255', 31); -- // 24 bit IP4-prefix is used for comparison
print ipv6_is_match('192.168.1.1',    '192.168.1.255', 24); -- // 24 bit IP4-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994', 'fe80::85d:e82c:9446:7995',     127); -- // 127 bit IP6-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994/127', 'fe80::85d:e82c:9446:7998', 120); -- // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('fe80::85d:e82c:9446:7994/120', 'fe80::85d:e82c:9446:7998', 127); -- // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('192.168.1.1/24',   '::ffff:c0a8:01ff', 127); -- // 127 bit IP6-prefix is used for comparison
print ipv6_is_match('::ffff:c0a8:0101', '192.168.1.255',    120); -- // 120 bit IP6-prefix is used for comparison
print ipv6_is_match('::192.168.1.1/30', '192.168.1.255/24', 127); -- // 120 bit IP6-prefix is used for comparison
