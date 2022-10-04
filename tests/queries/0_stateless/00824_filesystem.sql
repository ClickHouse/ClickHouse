SELECT filesystemCapacity() >= filesystemAvailable() AND filesystemAvailable() >= filesystemUnreserved() AND filesystemUnreserved() >= 0;
