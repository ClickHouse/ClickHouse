export const Image = ({ img, alt, size }) => {
  return (
    <Frame>
      <img src={img} alt={alt} />
    </Frame>
  );
};