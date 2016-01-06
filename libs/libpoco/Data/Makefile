#
# Makefile
#
# $Id: //poco/Main/Data/Makefile#11 $
#
# Makefile for Poco Data
#

include $(POCO_BASE)/build/rules/global

objects = AbstractBinder AbstractBinding AbstractExtraction AbstractExtractor \
	AbstractPreparation AbstractPreparator ArchiveStrategy Transaction \
	Bulk Connector DataException Date DynamicLOB Limit MetaColumn \
	PooledSessionHolder PooledSessionImpl Position \
	Range RecordSet Row RowFilter RowFormatter RowIterator \
	SimpleRowFormatter Session SessionFactory SessionImpl \
	SessionPool SessionPoolContainer SQLChannel \
	Statement StatementCreator StatementImpl Time

target         = PocoData
target_version = $(LIBVERSION)
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/lib
