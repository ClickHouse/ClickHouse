# Learnings

- `serialization_info_version = 'with_physical_names'` (version 2) has a pre-existing bug: Nested columns added via ALTER produce zero data on read. The offset stream is found (`n.size0`) but data stream resolution fails. This is independent of the physical name mapping. Affects both flattened and unflattened Nested types.

- For non-replicated MergeTree, the safe ordering is: persist `physical_names.json` BEFORE metadata commit (`alterTable`). If we crash between the two, the mapping has stale entries that are harmless (identity fallback for columns not yet in metadata). The old approach (metadata first, mapping second) leaves a window where crash recovery creates wrong identity mappings.

- Flattened Nested `ADD COLUMN n Nested(...)` with `flatten_nested = 1` expands into `n.x`, `n.y` in metadata but the command only carries `command.column_name = "n"`. The physical name mapping must use metadata diff (old vs new columns) to discover the actual expanded subcolumn names.

- `AlterCommand::isRequireMutationStage` has `clear` and `partition` fields on the command. When skipping mutation for `DROP_COLUMN` with physical names active, only the plain drop (`!clear && !partition`) should be metadata-only. The partition/clear variants still need mutations.

- `SharedMergeTree` constructor calls `loadPhysicalNameMappingFromDisk()` before assigning `relative_data_path`, causing the disk fallback to read from an empty path. The fix is to assign `relative_data_path = init_data.path_on_disk` before the physical names loading block.

- Physical-name flattened Nested subcolumns (e.g. `n.x`, `n.y`) use flat `NameAndTypePair` for correct `getFileNameForStreamPhysical` resolution but must NOT share `SubstreamsCache` in `MergeTreeReaderWide`. In the normal case, the Nested subcolumn serialization adds `TupleElement` to substream paths, making data cache keys unique ("x", "y") while sharing offset key ("size0"). Flat columns lack `TupleElement`, so both inner data caches key to `""` — sharing the cache causes `n.y` to find `n.x`'s UInt64 column as its String data. Fix: keep separate caches but `seekToStart` on shared offset stream for siblings.

- `RENAME COLUMN` with physical names requires a two-phase approach for crash safety: `beginRename` keeps both old and new logical names in the mapping before persist, `finishRename` removes the old entry after metadata commit. Without this, a crash between mapping persist and metadata commit loses the physical name for columns with non-identity mappings (numeric physical names).

- `PhysicalNameMapping::removeColumn` must check if another logical name maps to the same physical name before erasing from `physical_to_logical` — this happens during the two-phase rename window or when reconciliation removes stale entries. Similarly, `fromString` deserialization must tolerate duplicate physical names.
