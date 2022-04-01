def corrupt_part_data_on_disk(node, table, part_name):
    part_path = node.query("SELECT path FROM system.parts WHERE table = '{}' and name = '{}'"
        .format(table, part_name)).strip()

    corrupt_part_data_by_path(node, part_path)

def corrupt_part_data_by_path(node, part_path):
    print("Corrupting part", part_path, "at", node.name)
    print("Will corrupt: ",
        node.exec_in_container(['bash', '-c', 'cd {p} && ls *.bin | head -n 1'.format(p=part_path)]))

    node.exec_in_container(['bash', '-c',
                            'cd {p} && ls *.bin | head -n 1 | xargs -I{{}} sh -c \'echo "1" >> $1\' -- {{}}'.format(
                                p=part_path)], privileged=True)
