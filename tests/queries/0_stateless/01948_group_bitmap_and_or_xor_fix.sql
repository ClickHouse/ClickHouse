SELECT groupBitmapAnd(bitmapBuild([toInt32(1)])), groupBitmapOr(bitmapBuild([toInt32(1)])), groupBitmapXor(bitmapBuild([toInt32(1)])) FROM cluster(test_cluster_two_shards, numbers(10));
