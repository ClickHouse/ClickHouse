## FST dump tree

This tool allows to inspect the number of states and dump the FST tree (`text` index) in a dotgraph representation.

### The simple usage without additional arguments:
```sh
$ clickhouse-fst-dump-tree --input /data/database/table/table_part
Reading FST index files from '/data/database/table/table_part'
Segment version = 1 and number of segments = 1
[Segment 0]: FST size = 20.45 MiB
```

The command without additional arguments just prints how many segments there are in FST and total size of the FST blob.

### To print FST tree dotgraph representation, `--dot` argument can be used:
```sh
$ clickhouse-fst-dump-tree --input /data/database/table/table_part --dot
Reading FST index files from '/data/database/table/table_part'
Segment version = 1 and number of segments = 1
[Segment 0]: FST size = 46.00 B
[Segment 0]: FST as dotgraph:
digraph {state_37[label="State off: 37"];state_33[label="State off: 33"];state_29[label="State off: 29"];state_25[label="State off: 25"];state_21[label="State off: 21"];state_0[label="State off: 0",shape=doublecircle];state_21 -> state_0[label="d | 0"];state_25 -> state_21[label="l | 0"];state_29 -> state_25[label="r | 0"];state_33 -> state_29[label="o | 0"];state_37 -> state_33[label="W | 4"];state_14[label="State off: 14"];state_10[label="State off: 10"];state_6[label="State off: 6"];state_2[label="State off: 2"];state_0[label="State off: 0",shape=doublecircle];state_2 -> state_0[label="o | 0"];state_6 -> state_2[label="l | 0"];state_10 -> state_6[label="l | 0"];state_14 -> state_10[label="e | 2"];state_10[label="State off: 10"];state_37 -> state_14[label="H | 0"];}
```

### To save FST tree dotgraph representation into disk, `--output` argument with the output path can be used:
```sh
$ clickhouse-fst-dump-tree --input /data/database/table/table_part --output /tmp
Reading FST index files from '/data/database/table/table_part'
Segment version = 1 and number of segments = 1
[Segment 0]: FST size = 46.00 B
[Segment 0]: FST as dotgraph saved into /tmp/segment_0.dot
```

### To print FST tree states information, `--states` argument can be used:
```sh
$ clickhouse-fst-dump-tree --input /data/database/table/table_part --states
Reading FST index files from '/data/database/table/table_part'
Segment version = 1 and number of segments = 1
[Segment 0]: FST size = 46.00 B
[Segment 0][State off 0]: size = 2.00 B | labels = 0 | encoding = 'sequential'
[Segment 0][State off 2]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 6]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 10]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 14]: size = 7.00 B | labels = 2 | encoding = 'sequential'
[Segment 0][State off 21]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 25]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 29]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 33]: size = 4.00 B | labels = 1 | encoding = 'sequential'
[Segment 0][State off 37]: size = 7.00 B | labels = 2 | encoding = 'sequential'
[Segment 0]: FST number of states = 10
[Segment 0]: FST start state offset = 37
```

When `--states` argument is provided, it prints information about of all states. This information includes the state offset in FST blob, total size, number of outgoing labels, outgoing labels (`--labels` argument is required) and encoding used to store labels in FST.

### To print FST tree states information and output labels from each state, `--labels` argument can be used (it must be used together with `--states`):
```sh
$ clickhouse-fst-dump-tree --input /data/database/table/table_part --states --labels
Reading FST index files from '/data/database/table/table_part'
Segment version = 1 and number of segments = 1
[Segment 0]: FST size = 39.00 B
[Segment 0][State off 0]: size = 2.00 B | labels = 0 ['e'] | encoding = 'sequential'
[Segment 0][State off 2]: size = 4.00 B | labels = 1 ['s'] | encoding = 'sequential'
[Segment 0][State off 6]: size = 4.00 B | labels = 1 ['u'] | encoding = 'sequential'
[Segment 0][State off 10]: size = 4.00 B | labels = 1 ['o'] | encoding = 'sequential'
[Segment 0][State off 14]: size = 4.00 B | labels = 1 ['h'] | encoding = 'sequential'
[Segment 0][State off 18]: size = 4.00 B | labels = 1 ['k'] | encoding = 'sequential'
[Segment 0][State off 22]: size = 4.00 B | labels = 1 ['c'] | encoding = 'sequential'
[Segment 0][State off 26]: size = 4.00 B | labels = 1 ['i'] | encoding = 'sequential'
[Segment 0][State off 30]: size = 4.00 B | labels = 1 ['l'] | encoding = 'sequential'
[Segment 0][State off 34]: size = 4.00 B | labels = 1 ['c'] | encoding = 'sequential'
[Segment 0]: FST number of states = 10
[Segment 0]: FST start state offset = 34
```

arguments can be mixed.

### Note:
The `input` path should point to the table part. You can get paths for all parts of your table via:
```sql
SELECT path FROM system.parts WHERE database = 'your-database' AND table = 'your-table';
```

Currently, this tool supports to dump information about FST of a single table part.
