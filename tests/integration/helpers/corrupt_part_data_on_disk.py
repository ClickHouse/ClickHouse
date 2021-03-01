def corrupt_part_data_on_disk(node, table, part_name, is_detached=False):
    parts_table = "system.detached_parts" is is_detached else "system.parts"
    part_path = node.query(
        "SELECT path FROM " + parts_table + " WHERE table = '{}' and name = '{}'".format(table, part_name)).strip()
    node.exec_in_container(['bash', '-c',
                            'cd {p} && ls *.bin | head -n 1 | xargs -I{{}} sh -c \'echo "1" >> $1\' -- {{}}'.format(
                                p=part_path)], privileged=True)
