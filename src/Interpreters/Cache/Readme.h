#pragma once

/* (0) FileCache -> (1) CacheMetadata
 *
 * (1) CacheMetadata -> (4)
 * (2) LockedKey -> (4)
 * (3) IFileCachePriority::Entry -> (4) KeyMetadata -> (5) FileSegmentMetadata -> (6) FileSegmentPtr
 *                                                                           |--> (7) IFileCachePriority::Iterator (LRUFileCachePriority::LRUFileCacheIterator)
 *
 * (7) -> (8) LRUFileCachePriority* -> (9) LRUFileCachePriority::LRUQueue -> (3) IFileCachePriority::Entry
 * 
 * (6) -> (10) FileSegment -> (4) KeyMetadata* -> 
 *                       |--> (0) FileCache*
 *
 */
