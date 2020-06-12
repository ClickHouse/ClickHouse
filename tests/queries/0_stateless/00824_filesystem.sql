SELECT filesystemCapacity() >= filesystemFree() AND filesystemFree() >= filesystemAvailable() AND filesystemAvailable() >= 0;
