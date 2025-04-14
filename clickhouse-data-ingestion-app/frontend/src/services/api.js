import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || '/api';

// Create axios instance
const axiosInstance = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Connection API
export const connectToClickHouse = async (connectionData) => {
  try {
    const response = await axiosInstance.post('/connections/clickhouse', connectionData);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

export const configureFileConnection = async (connectionData) => {
  try {
    const response = await axiosInstance.post('/connections/file', connectionData);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

export const closeConnection = async (connectionId) => {
  try {
    const response = await axiosInstance.delete(`/connections/${connectionId}`);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

// Schema API
export const discoverClickHouseSchema = async (schemaRequest) => {
  try {
    const response = await axiosInstance.post('/schemas/clickhouse', schemaRequest);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

export const discoverFileSchema = async (schemaRequest) => {
  try {
    const response = await axiosInstance.post('/schemas/file', schemaRequest);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

// Data API
export const previewData = async (previewRequest) => {
  try {
    const response = await axiosInstance.post('/data/preview', previewRequest);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

export const transferData = async (transferRequest) => {
  try {
    const response = await axiosInstance.post('/data/transfer', transferRequest);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
};

export const getTransferProgress = async (transferId) => {
  try {
    const response = await axiosInstance.get(`/data/transfer/${transferId}/progress`);
    return response.data;
  } catch (error) {
    throw error.response ? error.response.data : error;
  }
}; 