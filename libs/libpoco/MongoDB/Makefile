#
# Makefile
#
# $Id$
#
# Makefile for Poco MongoDB
#

include $(POCO_BASE)/build/rules/global

INCLUDE += -I $(POCO_BASE)/MongoDB/include/Poco/MongoDB

objects = Array Binary Connection Cursor DeleteRequest  Database \
	Document Element GetMoreRequest InsertRequest JavaScriptCode \
	KillCursorsRequest Message MessageHeader ObjectId QueryRequest \
	RegularExpression ReplicaSet RequestMessage ResponseMessage \
	UpdateRequest

target         = PocoMongoDB
target_version = $(LIBVERSION)
target_libs    = PocoFoundation PocoNet

include $(POCO_BASE)/build/rules/lib
